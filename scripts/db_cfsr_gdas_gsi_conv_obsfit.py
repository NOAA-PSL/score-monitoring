#!/usr/bin/env python

"""Copyright 2025 NOAA
All rights reserved.

This script calls database harvesting for the given files, statistics, and
variables at the top of the script. score-db makes the harvesting call,
translates, and stores the data as experiment metrics.

This script assumes that the statistics and variables provided are already
registered as metric types.

This script relies on environment variables for the S3 bucket and the location
of the score-db executable. Folder structure is assumed to be BUCKET/KEY/files.
"""

import sys
import shutil
#import boto3
#from botocore import UNSIGNED
#from botocore.client import Config
#from botocore.errorfactory import ClientError
import db_yaml_generator 
import os
import pathlib
import datetime as dt
from dotenv import load_dotenv

from score_db import score_db_base
from score_db import file_utils
from edcfsr3 import edcfsr

def get_ensemble_member(ensemble_member='control'):
    #TODO: implement method to retrieve ensemble member dynamically
    return ensemble_member

#stats and variables passed in for harvest
variables = [
    'fit_uv_data', # fit of u, v wind data (m/s),
    'fit_t_data', # fit of temperature data (K)
]

psfc_variables = ['fit_psfc_data', # fit of surface pressure data (mb)
]
qsat_variables = ['fit_q_data', # fit of moisture data (% of qsaturation guess)
] 

statistics = [
    'count', # number of obs summed under obs types and vertical layers
    'bias', # bias of obs departure for each outer loop (it)
    'rms', # root mean squre error of obs departure for each outer loop (it)
    #'cpen', # obs part of penalty (cost function)
    #'qcpen' # nonlinear qc penalty
]

psfc_plev_bounds = [[0.200E+04, 0.000E+00]]

plev_bounds = [
        [1200.0, 1000.0],
        [999.9, 900.0],
        [899.9, 800.0],
        [799.9, 600.0],
        [599.9, 400.0],
        [399.9, 300.0],
        [299.9, 250.0],
        [249.9, 200.0],
        [199.9, 150.0],
        [149.9, 100.0],
        [99.9, 50.0],
        [2000.0, 0.0],
]
    
qsat_plev_bounds = [
        [1200.0, 1000.0],
        [999.9, 950.0],
        [949.9, 900.0],
        [899.9, 850.0],
        [849.9, 800.0],
        [799.9, 700.0],
        [699.9, 600.0],
        [599.9, 500.0],
        [499.9, 400.0],
        [399.9, 300.0],
        [299.9, 0.0],
        [2000.0, 0.0],
]

input_cycle = sys.argv[1]
datetime_obj = dt.datetime.strptime(input_cycle, "%Y%m%dT%H")
datetime_str = datetime_obj.strftime("%Y%m%d%H")
cycle_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

ensemble_member = get_ensemble_member()

input_env = sys.argv[2]
env_path = os.path.join(pathlib.Path(__file__).parent.parent.resolve(), input_env)
load_dotenv(env_path)

#aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
#aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
gsi_fit_file_name_format = os.getenv('GSI_FIT_FILE_NAME_FORMAT')
gsi_fit_file_key = os.getenv('GSI_FIT_FILE_KEY')

if gsi_fit_file_name_format == '' or gsi_fit_file_name_format == None:
    raise ValueError('Did not receive a GSI fit file format. Please '
                     'specify a format for the GSI fit file in your '
                     'environment configuration file')
    
#if aws_access_key_id == '' or aws_access_key_id == None:
    # move forward with unsigned request
#    s3_config_signature_version = UNSIGNED
#else:
#    s3_config_signature_version = 's3v4'

#s3 = boto3.resource(
#    's3',
#    aws_access_key_id=aws_access_key_id,    
#    aws_secret_access_key=aws_secret_access_key, 
#    config=Config(signature_version=s3_config_signature_version))

#bucket = s3.Bucket(os.getenv('STORAGE_LOCATION_BUCKET'))

if gsi_fit_file_key == '' or gsi_fit_file_key == None:
    prefix = datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/")
else:
    prefix = datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/" + gsi_fit_file_key + "/")

file_name = dt.datetime.strftime(datetime_obj,
                                 format = gsi_fit_file_name_format)

work_dir = os.getenv('CYLC_TASK_WORK_DIR')
file_path =  os.path.join(work_dir, f'gsistats.{datetime_str}_{ensemble_member}')
try:
    edcfsr(prefix + file_name, file_path)
    #shutil.copy(prefix + file_name, file_path)
    #bucket.download_file(prefix + file_name, file_path)
except ClientError as err:
    if err.response['Error']['Code'] == "404":
        print(f"File {file_name} not found at {prefix}")
        print(err)
        raise err
    else:
        print(err)
        raise err

#harvest: build harvest config, build yaml, call subprocess, statistic/variable 
#combo needs to be registered to be saved in db
harvest_config = {'harvester_name': 'gsi_conventional_obs',
                     'filename': file_path,
                     'variables': variables,
                     'statistics': statistics,
                     'plev_bounds': plev_bounds}
qsat_harvest_config = {'harvester_name': 'gsi_conventional_obs',
                       'filename': file_path,
                       'variables': qsat_variables,
                       'statistics': statistics,
                       'plev_bounds': qsat_plev_bounds}
psfc_harvest_config = {'harvester_name': 'gsi_conventional_obs',
                       'filename': file_path,
                       'variables': psfc_variables,
                       'statistics': statistics,
                       'plev_bounds': psfc_plev_bounds}       

psfc_yaml_file = db_yaml_generator.generate_harvest_metrics_yaml(
                                        os.getenv('EXPERIMENT_NAME'),
                                        os.getenv('EXPERIMENT_WALLCLOCK_START'),
                                        'gsi_conventional_obs',
                                        psfc_harvest_config,
                                        )
# validate the configuration (yaml) file
file_utils.is_valid_readable_file(psfc_yaml_file)
# submit the score db request
print("Calling score-db with yaml file: " + psfc_yaml_file + "for cycle: " +
      cycle_str)

response = score_db_base.handle_request(psfc_yaml_file)
if not response.success:
    print(response.message)
    print(response.errors)
    raise RuntimeError("score-db returned a failure message") #generic exception to tell cylc to stop running

yaml_file = db_yaml_generator.generate_harvest_metrics_yaml(
                                        os.getenv('EXPERIMENT_NAME'),
                                        os.getenv('EXPERIMENT_WALLCLOCK_START'),
                                        'gsi_conventional_obs',
                                        harvest_config,
                                        is_array=True)
# validate the configuration (yaml) file
file_utils.is_valid_readable_file(yaml_file)
# submit the score db request
print("Calling score-db with yaml file: " + yaml_file + "for cycle: " +
      cycle_str)

response = score_db_base.handle_request(yaml_file)
if not response.success:
    print(response.message)
    print(response.errors)
    raise RuntimeError("score-db returned a failure message") #generic exception to tell cylc to stop running
    
qsat_yaml_file = db_yaml_generator.generate_harvest_metrics_yaml(
                                        os.getenv('EXPERIMENT_NAME'),
                                        os.getenv('EXPERIMENT_WALLCLOCK_START'),
                                        'gsi_conventional_obs',
                                        qsat_harvest_config,
                                        is_array=True)
# validate the configuration (yaml) file
file_utils.is_valid_readable_file(qsat_yaml_file)
# submit the score db request
print("Calling score-db with yaml file: " + qsat_yaml_file + "for cycle: " +
      cycle_str)

response = score_db_base.handle_request(qsat_yaml_file)
if not response.success:
    print(response.message)
    print(response.errors)
    raise RuntimeError("score-db returned a failure message") #generic exception to tell cylc to stop running
