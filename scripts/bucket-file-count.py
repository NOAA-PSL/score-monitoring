import sys
import boto3
import datetime as dt
from botocore import UNSIGNED
from botocore.client import Config
from dotenv import load_dotenv
import os

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
latest = dt.datetime(1, 1, 1, tzinfo=dt.timezone.utc)
files = bucket.objects.filter(Prefix=prefix)
for file in files: 
    file_count+=1
    if latest < file.last_modified:
        latest = file.last_modified

if file_count is 0:
    raise Exception("no files found in bucket " + datetime_str)

diff = dt.datetime.now(dt.timezone.utc) - latest 
diff_minutes = diff.total_seconds() / 60

print("minutes: ")
print(diff_minutes) 
if diff_minutes < 30:
    raise Exception("the latest file is more recent than 30 minutes, try again later")

print("last modified:")
print(latest)

print("File count: ")
print(file_count)
