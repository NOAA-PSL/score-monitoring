#!/usr/bin/env python

"""Copyright 2025 NOAA
All rights reserved.

This script calls database harvesting for the given files, statistics, and
variables at the top of the script. score-db makes the harvesting call,
translates, and stores the data as experiment metrics.

This script assumes that the statistics and variables provided are already
registered as metric types.

This script relies on environment variables for the S3 bucket and the location
of the score-db executable. Folder structure is assumed to be BUCKET/KEY/files.
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

#stats and variables passed in for harvest

score_hv_harvester = 'soca_diags'

variables = [#'sst',
             #'icec',
             #'salinity',
             'waterTemperature',
             #'seaSurfaceSalinity',
             #'seaSurfaceTemperature'
             ]
statistics = ['rms', 'mean', 'median','StdDev', 'minimum', 'maximum', 'count']

input_cycle = sys.argv[1]
datetime_obj = dt.datetime.strptime(input_cycle, "%Y%m%dT%H")
datetime_str = datetime_obj.strftime("%Y%m%d%H")
cycle_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

input_env = sys.argv[2]
env_path = os.path.join(pathlib.Path(__file__).parent.parent.resolve(), input_env)
load_dotenv(env_path)

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
soca_diags_dir_name_format = os.getenv('SOCA_DIAGS_DIR_NAME_FORMAT')

if soca_diags_dir_name_format == '' or soca_diags_dir_name_format == None:
    raise ValueError('Did not receive a SOCA diags directory format. Please '
                     'specify the AWS path to the SOCA diag files in your '
                     'environment configuration file')
    
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
prefix = datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/")

#dir_name = dt.datetime.strftime(datetime_obj,
#                                 format = soca_diags_dir_name_format)

work_dir = os.getenv('CYLC_TASK_WORK_DIR')
file_path_list = list()
for obj in bucket.objects.filter(Prefix=prefix):
    file_path_list.append(os.path.join(work_dir, obj.key))
    try:
        bucket.download_file(obj.key, file_path)
    except ClientError as err:
        if err.response['Error']['Code'] == "404":
            print(f"File {file_name} not found at {prefix}")
            print(err)
            raise err
        else:
            print(err)
            raise err

#harvest: build harvest config, build yaml, call subprocess, statistic/variable 
#combo needs to be registered to be saved in db
harvest_config = {'harvester_name': score_hv_harvester,
                     'filename': file_path_list,
                     'variables': variables,
                     'statistics': statistics}
yaml_file = db_yaml_generator.generate_harvest_metrics_yaml(
                                        os.getenv('EXPERIMENT_NAME'),
                                        os.getenv('EXPERIMENT_WALLCLOCK_START'),
                                        score_hv_harvester,
                                        harvest_config,
                                        is_array=False)
# validate the configuration (yaml) file
file_utils.is_valid_readable_file(yaml_file)
# submit the score db request
print("Calling score-db with yaml file: " + yaml_file + "for cycle: " +
      cycle_str)

response = score_db_base.handle_request(yaml_file)
if not response.success:
    print(response.message)
    print(response.errors)
    raise RuntimeError("score-db returned a failure message") #generic exception to tell cylc to stop running
