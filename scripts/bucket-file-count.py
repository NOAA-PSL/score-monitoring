import sys
import boto3
from botocore import UNSIGNED
from botocore.client import Config

print("Arg value: ")
print(sys.argv[1])

year = "1998"
month = "02"
datetime = "1998022106"

s3 = boto3.resource(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='',
    config=Config(signature_version=UNSIGNED)
)
bucket = s3.Bucket('noaa-ufs-gefsv13replay-pds')

prefix = "spinup/" + year + "/" + month + "/" + datetime + "/"
file_list = bucket.objects.filter(Prefix=prefix)
file_count = len(file_list)
print("File count: ")
print(file_count)
