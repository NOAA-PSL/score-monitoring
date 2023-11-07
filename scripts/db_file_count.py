"""
Copyright 2023 NOAA
All rights reserved.

This script counts the number of files in a given S3 bucket and saves the value in a database.
This script relies on environment variables for the S3 bucket and the location of the score-db executable.
Folder structure is assumed to be KEY/%Y/%M/CYCLE.
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
import subprocess


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

s3 = boto3.resource(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='',
    config=Config(signature_version=UNSIGNED)
)

bucket = s3.Bucket(os.getenv('STORAGE_LOCATION_BUCKET'))
key = os.getenv('STORAGE_LOCATION_KEY')
if key:
    prefix = key + "/" + year + "/" + month + "/" + datetime_str + "/"
else: 
    prefix = year + "/" + month + "/" + datetime_str + "/"
    
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

print("Calling score-db with yaml file: " + yaml_file + "for cycle: " + cycle_str)
try:
    run = subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file], check=True, capture_output = True, text=True)
    stdout = run.stdout
    stderr = run.stderr
    print("standard out")
    print(stdout)
    print("standard err")
    print(stderr)
    print(stderr, file=sys.stderr)
except subprocess.CalledProcessError as err:
    print("An error occurred while running score-db")
    print("err std out")
    print(err.stdout)
    print(err.stderr, file=sys.stderr)
os.remove(yaml_file)
