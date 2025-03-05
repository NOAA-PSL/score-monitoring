#!/usr/bin/env python

"""
Copyright 2025 NOAA
All rights reserved.

This script checks if files exist and are older than 30 minutes 
for the given cycle in the S3 storage bucket provided in the environment variables.
It assumes a folder structure of: BUCKET/KEY/files
"""

import sys
import boto3
import datetime as dt
from botocore import UNSIGNED
from botocore.client import Config
from dotenv import load_dotenv
import os
import pathlib

input_cycle = sys.argv[1]
datetime_obj = dt.datetime.strptime(input_cycle, "%Y%m%dT%H")
datetime_str = datetime_obj.strftime("%Y%m%d%H")

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

prefix = datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/")

file_count = 0
latest = dt.datetime(1, 1, 1, tzinfo=dt.timezone.utc)
files = bucket.objects.filter(Prefix=prefix)
for file in files: 
    file_count+=1
    if latest < file.last_modified:
        latest = file.last_modified

if file_count is 0:
    raise Exception("no files found in bucket " + prefix)

diff = dt.datetime.now(dt.timezone.utc) - latest 
diff_minutes = diff.total_seconds() / 60

if diff_minutes < 30:
    raise Exception("the latest file is more recent than 30 minutes, try again later")

print("File count: ")
print(file_count)