"""
Copyright 2023 NOAA
All rights reserved.

This script is currently only applicable to REPLAY experiments due to the specific cloud based 
file names, formats, and harvester being used. 

This script calls database harvesting for the given files, statistics, and variables at the top of
the script. score-db makes the harvesting call, translates, and stores the data as experiment metrics.

This script assumes that the statistics and variables provided are already registered as metric types.

This script relies on environment variables for the S3 bucket and the location of the score-db executable.
Folder structure is assumed to be KEY/%Y/%M/CYCLE/logs.
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

#DICTIONARIES
#file list needed to harvest
file_list = {
    'calc_atm_inc.out',
    'calc_ocn_inc.out'
}
#stats and variables passed in for harvest
statistics = ['mean', 'RMS']
variables = ['o3mr_inc', 'sphum_inc', 'T_inc', 'u_inc', 'v_inc',
                                  'delp_inc', 'delz_inc', 'pt_inc', 's_inc', 'u_inc', 'v_inc', 'SSH',
                                  'Salinity', 'Temperature', 'Speed of Currents']


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
env_path = os.path.join(pathlib.Path(__file__).parent.parent.resolve(), input_env)
load_dotenv(env_path)

s3 = boto3.resource(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='',
    config=Config(signature_version=UNSIGNED)
)

bucket = s3.Bucket(os.getenv('STORAGE_LOCATION_BUCKET'))

prefix = datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/logs/")

work_dir = os.getenv('WORK_DIR')
if work_dir is None:
    work_dir = pathlib.Path(__file__).parent.resolve()

#harvester is built to handle one file at a time so make calls per listed file 
for file in file_list:
    #download file using unique name for each cycle 
    file_path =  os.path.join(work_dir,cycle_str + "-" + file)

    try:
        bucket.download_file(prefix+file, file_path)
    except ClientError as err:
        if err.response['Error']['Code'] == "404":
            print(f"File {file} not found at {prefix}. Moving on to the next file in list")
            print(err)
            continue
        else:
            print(err)
            raise err

    #harvest: build harvest config, build yaml, call subprocess, statistic/variable combo needs to be registered to be saved in db
    harvest_config = {
        'harvester_name': 'inc_logs',
        'filename': file_path, 
        'statistic': statistics,
        'variable': variables,
        'cycletime': cycle_str
    }
    yaml_file = db_yaml_generator.generate_harvest_metrics_yaml(os.getenv('EXPERIMENT_NAME'), os.getenv('EXPERIMENT_WALLCLOCK_START'),
                                                    'inc_logs', harvest_config)
    
    # validate configuration (yaml) file
    file_utils.is_valid_readable_file(yaml_file)
    # submit the score db request    
    print("Calling score-db with yaml file: " + yaml_file + "for cycle: " + cycle_str)
    response = score_db_base.handle_request(yaml_file)
    if not response.success:
        print(response.message)
        print(response.errors)
        raise RuntimeError("score-db returned a failure message") #generic exception to tell cylc to stop running 

    #remove yaml and downloaded file 
    os.remove(yaml_file)
    os.remove(file_path)
    print(f"Finished with file {file} at {prefix}") 
