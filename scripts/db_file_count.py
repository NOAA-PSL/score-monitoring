import sys
import boto3
import datetime as dt
from botocore import UNSIGNED
from botocore.client import Config
import db_yaml_generator 
import os
import pathlib

from dotenv import load_dotenv
import subprocess


print("Arg value: ")
print(sys.argv[1])

input = sys.argv[1]
datetime_obj = dt.datetime.strptime(input, "%Y%m%dT%H")
year = datetime_obj.strftime("%Y")
month = datetime_obj.strftime("%m")
datetime_str = datetime_obj.strftime("%Y%m%d%H")
cycle_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

load_dotenv()

s3 = boto3.resource(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='',
    config=Config(signature_version=UNSIGNED)
)

bucket = s3.Bucket(os.getenv('S3_BUCKET'))
prefix = "spinup/" + year + "/" + month + "/" + datetime_str + "/"

file_type = 'all_files'

file_count = 0
files = bucket.objects.filter(Prefix=prefix)
for file in files: 
    file_count+=1

if file_count is 0:
    raise Exception("no files found in bucket " + datetime_str)

print("File count: ")
print(file_count)

now = dt.now().strftime("%Y-%m-%d %H:%M:%S")
yaml_file = db_yaml_generator.generate_file_count_yaml(file_count, file_type, now, prefix, cycle_str)

subprocess.run(["python", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file])
#score-db always returns the same type of response so it can be easily checked for success and error messages
# should we raise an exception? allow for a retry? or just document the error somewhere? 
