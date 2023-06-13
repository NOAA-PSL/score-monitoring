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
prefix = os.getenv('STORAGE_LOCATION_KEY') + "/" + year + "/" + month + "/" + datetime_str + "/"

#file list needed to harvest
file_list = {
    'calc_atm_inc.out',
    'calc_ocn_inc.out'
}

#harvester is built to handle one file at a time so make calls per listed file 
for file in file_list:
    #download file
    bucket.download_file(prefix, file)
    file_path =  os.path.join(pathlib.Path(__file__).parent.resolve(), file)

    #harvest: build harvest config, build yaml, call subprocess
    harvest_config = {
        'harvester_name': 'inc_logs',
        'filename': file_path, 
        'statistic': ['mean', 'RMS'],
        'variable': ['o3mr_inc', 'sphum_inc', 'T_inc', 'u_inc', 'v_inc',
                    'delp_inc', 'delz_inc']
    }
    yaml_file = db_yaml_generator.generate_harvest_metrics_yaml(os.getenv('EXPERIMENT_NAME'), os.getenv('EXPERIMENT_WALLCLOCK_START'),
                                                    'inc_logs', harvest_config)
    print("Calling score-db with yaml file: " + yaml_file + "for cycle: " + cycle_str)
    subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file], check=True)

    #remove yaml and downloaded file 
    os.remove(yaml_file)
    os.remove(file) 