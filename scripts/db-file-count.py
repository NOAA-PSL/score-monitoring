import sys
import boto3
import datetime as dt
from botocore import UNSIGNED
from botocore.client import Config
import db_yaml_generator 
import os
from dotenv import load_dotenv


print("Arg value: ")
print(sys.argv[1])

input = sys.argv[1]
datetime_obj = dt.datetime.strptime(input, "%Y%m%dT%H")
year = datetime_obj.strftime("%Y")
month = datetime_obj.strftime("%m")
datetime_str = datetime_obj.strftime("%Y%m%d%H")

load_dotenv()

s3 = boto3.resource(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='',
    config=Config(signature_version=UNSIGNED)
)

bucket = s3.Bucket(os.getenv('S3_BUCKET'))
prefix = "spinup/" + year + "/" + month + "/" + datetime_str + "/"

file_count = 0
files = bucket.objects.filter(Prefix=prefix)
for file in files: 
    file_count+=1

if file_count is 0:
    raise Exception("no files found in bucket " + datetime_str)

print("File count: ")
print(file_count)

# count, file_type, file_extension, time_valid, location_key
yaml_file = db_yaml_generator.generate_file_count_yaml(file_count, FILETYPE, EXTENSION, dt.now(), prefix)

# call score-db with this yaml file--- should this be an imported module ike hv into db?