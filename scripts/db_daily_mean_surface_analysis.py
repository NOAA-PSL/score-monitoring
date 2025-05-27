#!/usr/bin/env python

"""Copyright 2025 NOAA
All rights reserved.

This script calls database harvesting for the given files, statistics, and
variables at the top of the script. score-db makes the harvesting call,
translates, and stores the data as experiment metrics.

This script assumes that the statistics and variables provided are already
registered as metric types.

This script relies on environment variables for the S3 bucket and the location
of the score-db executable.
Folder structure is assumed to be BUCKET/KEY/files.
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

from score_db import score_db_base
from score_db import file_utils

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
             'lhtfl_ave', # surface latent heat flux (W m^-2)
             #'prate_ave', # surface precip rate (mm weq. s^-1)
             'prateb_ave', # bucket surface precip rate (mm weq. s^-1)
             'pressfc', # surface pressure (Pa)
             #'snod', # surface snow depth (m)
             #'soil4', # liquid soil moisture at layer-4 (?)
             #'soilm', # total column soil moisture content (mm weq.)
             #'soilt4', # soil temperature unknown layer 4 (K)
             #'tg3', # deep soil temperature (K)
             'tmp2m', # 2m (surface air) temperature (K)
             #'tmpsfc', # surface temperature (K)
             #'weasd', # surface snow water equivalent (mm weq.)
             ]

input_cycle = sys.argv[1]
datetime_obj = dt.datetime.strptime(input_cycle, "%Y%m%dT%H")
year = datetime_obj.strftime("%Y")
month = datetime_obj.strftime("%m")
datetime_str = datetime_obj.strftime("%Y%m%d%H")
cycle_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
datetime_obj_plus12h = datetime_obj + dt.timedelta(hours=12)

input_env = sys.argv[2]
env_path = os.path.join(pathlib.Path(__file__).parent.parent.resolve(), input_env)
load_dotenv(env_path)

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
if aws_access_key_id == '' or aws_access_key_id == None:
    # move forward with unsigned request
    s3_config_signature_version = UNSIGNED
else:
    s3_config_signature_version = 's3v4'

s3 = boto3.resource(
    's3',
    aws_access_key_id=aws_access_key_id,    
    aws_secret_access_key=aws_secret_access_key, 
    config=Config(signature_version=s3_config_signature_version))

bucket = s3.Bucket(os.getenv('STORAGE_LOCATION_BUCKET'))
key = os.getenv('STORAGE_LOCATION_KEY') + "/"

'''example file list needed to harvest (Jan 1 1994) daily mean from a 6 hour DA cycle:

file_name_list = ['bfg_1994010106_fhr03_control',
                  'bfg_1994010112_fhr00_control',
                  'bfg_1994010112_fhr03_control',
                  'bfg_1994010118_fhr00_control',
                  'bfg_1994010118_fhr03_control',
                  'bfg_1994010200_fhr00_control',
                  'bfg_1994010200_fhr03_control',
                  'bfg_1994010206_fhr00_control']
'''
prefix = list()
file_name_list = list()
for i in range(int(HOURS_PER_DAY/DA_WINDOW)):
    """Number of loops is the number of DA cycles per day
    """
    
    time_delta_fhr03 = dt.timedelta(hours = HOURS_PER_DAY - (i + 1) * DA_WINDOW)
    time_delta_fhr00 = dt.timedelta(hours = HOURS_PER_DAY - (i + 2) * DA_WINDOW)
    
    prefix.append(dt.datetime.strftime(datetime_obj_plus12h - time_delta_fhr03,
                                        format = key))
    prefix.append(dt.datetime.strftime(datetime_obj_plus12h - time_delta_fhr00,
                                        format = key))
    
    file_name_list.append(dt.datetime.strftime(datetime_obj_plus12h - time_delta_fhr03,
                                               format = 
                                               "bfg_%Y%m%d%H_fhr03_control"))
    
    file_name_list.append(dt.datetime.strftime(datetime_obj_plus12h - time_delta_fhr00,
                                               format = 
                                               "bfg_%Y%m%d%H_fhr00_control"))

work_dir = os.getenv('CYLC_TASK_WORK_DIR')
if work_dir is None:
    work_dir = pathlib.Path(__file__).parent.resolve()

file_path_list = list()
for i, file_name in enumerate(file_name_list):
    file_path =  os.path.join(work_dir, file_name)
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
                  'segment': 'analysis',
                  'statistic': statistics,
                  'variable': variables,}
yaml_file = db_yaml_generator.generate_harvest_metrics_yaml(
                                        os.getenv('EXPERIMENT_NAME'),
                                        os.getenv('EXPERIMENT_WALLCLOCK_START'),
                                        'daily_bfg',
                                        harvest_config)

# validate the configuration (yaml) file
file_utils.is_valid_readable_file(yaml_file)

# submit the score-db request
print("Calling score-db with yaml file: " + yaml_file + "for cycle: " +
      cycle_str)

response = score_db_base.handle_request(yaml_file)
if not response.success:
    print(response.message)
    print(response.errors)
    raise RuntimeError("score-db returned a failure message") #generic exception to tell cylc to stop running
