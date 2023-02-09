import sys
import boto3
from datetime import datetime
from botocore import UNSIGNED
from botocore.client import Config

print("Arg value: ")
print(sys.argv[1])

input = sys.argv[1]
datetime_obj =  datetime.strptime(input, "%Y%m%dT%H")
year = datetime_obj.strftime("%Y")
month = datetime_obj.strftime("%m")
datetime = datetime_obj.strftime("%Y%m%d%H")

s3 = boto3.resource(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='',
    config=Config(signature_version=UNSIGNED)
)
# bucket = s3.Bucket('noaa-ufs-gefsv13replay-pds')
bucket = s3.Bucket('noaa-reanalyses-pds')

# prefix = "spinup/" + year + "/" + month + "/" + datetime + "/"
prefix = "observations/reanalysis/ozone/nasa/sbuv_v87/" + year + "/" +  month + "/bufr/"
file_count = 0
files = bucket.objects.filter(Prefix=prefix)
for file in files: 
    file_count+=1

if file_count is 0:
    raise Exception("no files found in bucket " + datetime)

print("File count: ")
print(file_count)
