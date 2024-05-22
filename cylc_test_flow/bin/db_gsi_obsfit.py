#!/usr/bin/env python

"""Copyright 2023 NOAA
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

import score_db_base
import file_utils

#stats and variables passed in for harvest
variables = ()
statistics = ('bias_pre_corr', 'bias_post_corr', 'std')

#print("Arg value: ")
#print(sys.argv[1])
#print(sys.argv[2])

input_cycle = sys.argv[1]
datetime_obj = dt.datetime.strptime(input_cycle, "%Y%m%dT%H")
year = datetime_obj.strftime("%Y")
month = datetime_obj.strftime("%m")
datetime_str = datetime_obj.strftime("%Y%m%d%H")
cycle_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

input_env_path = sys.argv[2]
#env_path = os.path.join(pathlib.Path(__file__).parent.resolve(), input_env)
load_dotenv(input_env_path)

s3 = boto3.resource('s3', aws_access_key_id='', aws_secret_access_key='',
                    config=Config(signature_version=UNSIGNED))

bucket = s3.Bucket(os.getenv('STORAGE_LOCATION_BUCKET'))
key = os.getenv('STORAGE_LOCATION_KEY') + "/"
file_name = dt.datetime.strftime(datetime_obj, format = 'gsistats.%Y%m%d%h_control')

work_dir = os.getenv('CYLC_TASK_WORK_DIR')
file_path =  os.path.join(work_dir, file_name)
try:
    bucket.download_file(key + file_name, file_path)
except ClientError as err:
    if err.response['Error']['Code'] == "404":
        print(f"File {file_name} not found at {key}")
        print(err)
        raise err
    else:
        print(err)
        raise err

#harvest: build harvest config, build yaml, call subprocess, statistic/variable 
#combo needs to be registered to be saved in db
harvest_config = {'harvester_name': hv_registry.GSI_RADIANCE_CHANNEL,
                     'filename': file_path,
                     'variables': variables,
                     'statistics': statistics}
yaml_file = db_yaml_generator.generate_harvest_metrics_yaml(
                                        os.getenv('EXPERIMENT_NAME'),
                                        os.getenv('EXPERIMENT_WALLCLOCK_START'),
                                        'gsi_radiance_channel',
                                        harvest_config)
# validate the configuration (yaml) file
file_utils.is_valid_readable_file(yaml_file)
# submit the score db request
print("Calling score-db with yaml file: " + yaml_file + "for cycle: " +
      cycle_str)
score_db_base.handle_request(yaml_file)
