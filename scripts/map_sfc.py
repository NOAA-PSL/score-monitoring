#!/usr/bin/env python

"""
"""

import sys
from datetime import datetime
from dotenv import load_dotenv
import os
import pathlib

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from botocore.errorfactory import ClientError
import cartopy.crs as ccrs
from matplotlib import pyplot as plt
from netCDF4 import Dataset
import colorcet as cc

class SurfaceMapper(object):
    def __init__(self, input_cycle, input_env):
        """
        """
        self.parse_datetime(input_cycle)
        self.get_bucket(input_env)
        self.work_dir = os.getenv('CYLC_TASK_WORK_DIR')
        
        self.download_output_file()
        
    def parse_datetime(self, input_cycle):
        """
        """
        self.datetime_obj = datetime.strptime(input_cycle, "%Y%m%dT%H")
        self.datetime_str = datetime_obj.strftime("%Y%m%d%H")
        self.cycle_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    def get_bucket(self, input_env):
        """
        """
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
            config=Config(signature_version=s3_config_signature_version)
        )

        self.bucket = s3.Bucket(os.getenv('STORAGE_LOCATION_BUCKET'))
        
    def download_output_file(self,
                             fv3atm_file_key = os.getenv('FV3ATM_FILE_KEY'),
                             fv3sfc_file_name = os.getenv('FV3SFC_FILE_NAME')):
        """
        """
        if fv3atm_file_key == '' or fv3atm_file_key == None:
            prefix = self.datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/")
        else:
            prefix = self.datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/" + fv3atm_file_key + "/")

        target_file_name = datetime.strftime(self.datetime_obj,
                                             format = fv3sfc_file_name)

        self.dest_file_path = os.path.join(self.work_dir, target_file_name)
        try:
            self.bucket.download_file(prefix + target_file_name, self.dest_file_path)
        except ClientError as err:
            if err.response['Error']['Code'] == "404":
                print(f"File {target_file_name} not found at {prefix}")
                print(err)
                raise err
            else:
                print(err)
                raise err
                
    def view_surface(self, fv3atm_lw_var = 'ulwrf', fv3atm_sw_var = 'uswrf'):
        rootgrp = Dataset(self.dest_file_path)
        
        ax = plt.axes(projection=ccrs.Mercator(central_longitude=180.))
        
        ax.pcolormesh(C=rootgrp.variables[fv3atm_lw_var][:],
                      X=rootgrp.variables['lon'][:],
                      Y=rootgrp.variables['lat'][:],
                      cmap=cc.cmap.CET_L8,
                      shading='gouraud',
                      rasterized=True,
                      zorder=0,
                      )
        plt.savefig(
            f'{self.dest_file_path.split('/')[:-1]}sfc_{self.datetimestr}.png',
            dpi=300
        )
        
        rootgrp.close()

def run():
    """
    """
    surface_mapper = SurfaceMapper(sys.argv[1], sys.argv[2])
    surface_mapper.view_surface()

def main():
    run()

if __name__=='__main__':
    main()