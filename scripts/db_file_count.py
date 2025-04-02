#!/usr/bin/env python

"""
Copyright 2025 NOAA
All rights reserved.

This script counts the number of files in a given S3 bucket and saves the
value in a database. This script relies on environment variables for the S3
bucket and the location of the score-db executable. Folder structure is
assumed to be KEY/%Y/%M/CYCLE.
"""

import sys
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import db_yaml_generator 
import os
import pathlib
import datetime as dt
from dotenv import load_dotenv

from score_db import score_db_base
from score_db import file_utils

DEBUG=True

input_cycle = sys.argv[1]
datetime_obj = dt.datetime.strptime(input_cycle, "%Y%m%dT%H")
year = datetime_obj.strftime("%Y")
month = datetime_obj.strftime("%m")
datetime_str = datetime_obj.strftime("%Y%m%d%H")
cycle_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

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
key = os.getenv('STORAGE_LOCATION_KEY')

prefix = datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/")
    
file_type = 'all_files_example'

file_count = 0
files = bucket.objects.filter(Prefix=prefix)
for file in files: 
    file_count+=1

if file_count is 0:
    raise Exception("no files found in bucket " + datetime_str)

print("File count: ")
print(file_count)

yaml_file = db_yaml_generator.generate_file_count_yaml(file_count, file_type, None, None, prefix, cycle_str, 
                                                       os.getenv('EXPERIMENT_NAME'), os.getenv('EXPERIMENT_WALLCLOCK_START'),
                                                       os.getenv('STORAGE_LOCATION_BUCKET'), os.getenv('STORAGE_LOCATION_PLATFORM'), 
                                                       os.getenv('STORAGE_LOCATION_KEY'))

# validate configuration (yaml) file
file_utils.is_valid_readable_file(yaml_file)
# submit the score db request
print("Calling score-db with yaml file: " + yaml_file + "for cycle: " + cycle_str)

if not DEBUG:
    response = score_db_base.handle_request(yaml_file)
    if not response.success:
        print(response.message)
        print(response.errors)
        raise RuntimeError("score-db returned a failure message") #generic exception to tell cylc to stop running 