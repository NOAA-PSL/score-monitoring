import sys
import datetime
import boto3
from botocore import UNSIGNED
from botocore.client import Config

print("Arg value: ")
print(sys.argv[1])

input = sys.argv[1]
datetime_obj =  datetime.strptime(input, "%Y%m%dT%H")
# parse input into year, month, and datetime without the T 
year = datetime_obj.date.year
month = datetime_obj.date.month
datetime = datetime.strftime("%Y%m%d%H")

print("year: ")
print(year)
print("month: ")
print(month)
print("datetime: ")
print(datetime)

s3 = boto3.resource(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='',
    config=Config(signature_version=UNSIGNED)
)
bucket = s3.Bucket('noaa-ufs-gefsv13replay-pds')

prefix = "spinup/" + year + "/" + month + "/" + datetime + "/"
file_count = 0
files = bucket.objects.filter(Prefix=prefix)
for file in files: 
    file_count+=1
print("File count: ")
print(file_count)
