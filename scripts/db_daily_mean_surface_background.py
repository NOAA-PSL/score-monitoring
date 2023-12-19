"""Copyright 2023 NOAA
All rights reserved.

This script is currently only applicable to REPLAY experiments due to the
specific cloud based file names, formats, and harvester being used. 

This script calls database harvesting for the given files, statistics, and
variables at the top of the script. score-db makes the harvesting call,
translates, and stores the data as experiment metrics.

This script assumes that the statistics and variables provided are already
registered as metric types.

This script relies on environment variables for the S3 bucket and the location
of the score-db executable.
Folder structure is assumed to be KEY/%Y/%M/CYCLE.
"""

import sys
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from botocore.errorfactory import ClientError
import db_yaml_generator 
import os
import pathlib
import datetime as dt
from dotenv import load_dotenv
import subprocess

HOURS_PER_DAY = 24. # hours
DA_WINDOW = 6. # hours

#stats and variables passed in for harvest
statistics = ['mean', 'variance', 'minimum', 'maximum']

"""Variables of interest that come from the background forecast data are listed
below. Commented out variables could be uncommented to generate gridcell
weighted statistics but are in development and are currently not fully
supported.
"""
variables = [#'icetk', # sea ice thickness (m)
             #'lhtfl_ave', # surface latent heat flux (W m^-2)
             #'prate_ave', # surface precip rate (mm weq. s^-1)
             'prateb_ave', # bucket surface precip rate (mm weq. s^-1)
             #'pressfc', # surface pressure (Pa)
             #'snod', # surface snow depth (m)
             #'soil4', # liquid soil moisture at layer-4 (?)
             #'soilm', # total column soil moisture content (mm weq.)
             #'soilt4', # soil temperature unknown layer 4 (K)
             #'tg3', # deep soil temperature (K)
             #'tmp2m', # 2m (surface air) temperature (K)
             #'tmpsfc', # surface temperature (K)
             #'weasd', # surface snow water equivalent (mm weq.)
             ]

print("Arg value: ")
print(sys.argv[1])
print(sys.argv[2])

input_cycle = sys.argv[1]
datetime_obj = dt.datetime.strptime(input_cycle, "%Y%m%dT%H")
year = datetime_obj.strftime("%Y")
month = datetime_obj.strftime("%m")
datetime_str = datetime_obj.strftime("%Y%m%d%H")
cycle_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

input_env = sys.argv[2]
env_path = os.path.join(pathlib.Path(__file__).parent.resolve(), input_env)
load_dotenv(env_path)

s3 = boto3.resource('s3', aws_access_key_id='', aws_secret_access_key='',
                    config=Config(signature_version=UNSIGNED))

bucket = s3.Bucket(os.getenv('STORAGE_LOCATION_BUCKET'))
key = os.getenv('STORAGE_LOCATION_KEY')

'''example file list needed to harvest (Jan 1 1994) daily mean from a 6 hour DA cycle:

file_name_list = ['bfg_1994010100_fhr09_control',
                  'bfg_1994010106_fhr06_control',
                  'bfg_1994010106_fhr09_control',
                  'bfg_1994010112_fhr06_control',
                  'bfg_1994010112_fhr09_control',
                  'bfg_1994010118_fhr06_control',
                  'bfg_1994010118_fhr09_control',
                  'bfg_1994010200_fhr06_control']
'''
prefix = list()
file_name_list = list()
for i in range(int(HOURS_PER_DAY/DA_WINDOW)):
    """Number of loops is the number of DA cycles per day
    """
    
    time_delta_fhr09 = dt.timedelta(hours = HOURS_PER_DAY - i * DA_WINDOW)
    time_delta_fhr06 = dt.timedelta(hours = HOURS_PER_DAY - (i + 1) * DA_WINDOW)
    
    if key:
        prefix.append(dt.datetime.strftime(datetime_obj - time_delta_fhr09,
                                           format = key + "/%Y/%m/%Y%m%d%H/"))
        prefix.append(dt.datetime.strftime(datetime_obj - time_delta_fhr06,
                                           format = key + "/%Y/%m/%Y%m%d%H/"))
    else: 
        prefix.append(dt.datetime.strftime(datetime_obj - time_delta_fhr09,
                                           format = "%Y/%m/%Y%m%d%H/"))
        prefix.append(dt.datetime.strftime(datetime_obj - time_delta_fhr06,
                                           format = "%Y/%m/%Y%m%d%H/"))
    
    file_name_list.append(dt.datetime.strftime(datetime_obj - time_delta_fhr09,
                                               format = 
                                               "bfg_%Y%m%d%H_fhr09_control"))
    
    file_name_list.append(dt.datetime.strftime(datetime_obj - time_delta_fhr06,
                                               format = 
                                               "bfg_%Y%m%d%H_fhr06_control"))

file_path_list = list()
for i, file_name in enumerate(file_name_list):
    file_path =  os.path.join(pathlib.Path(__file__).parent.resolve(),
                              cycle_str + "-" + file_name)
    try:
        bucket.download_file(prefix[i] + file_name, file_path)
        file_path_list.append(file_path)
    except ClientError as err:
        if err.response['Error']['Code'] == "404":
            print(f"File {file_name} not found at {prefix[i]}. Moving on to the next file in list")
            print(err)
            raise err
        else:
            print(err)
            raise err

#harvest: build harvest config, build yaml, call subprocess, statistic/variable 
#combo needs to be registered to be saved in db
harvest_config = {'harvester_name': 'daily_bfg',
                  'filenames': file_path_list, 
                  'statistic': statistics,
                  'variable': variables,}
yaml_file = db_yaml_generator.generate_harvest_metrics_yaml(
                                        os.getenv('EXPERIMENT_NAME'),
                                        os.getenv('EXPERIMENT_WALLCLOCK_START'),
                                        'daily_bfg',
                                        harvest_config)
print("Calling score-db with yaml file: " + yaml_file + "for cycle: " +
      cycle_str)
subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file],
               check=True)

#remove yaml and downloaded files 
try:
    os.remove(yaml_file)
except FileNotFoundError:
    print('WARNING: FileNotFoundError raised during rm')

for i, file_path_to_remove in enumerate(file_path_list):
    os.remove(file_path_to_remove)
    print(f"Finished with file {file_name_list[i]} at {prefix[i]}")