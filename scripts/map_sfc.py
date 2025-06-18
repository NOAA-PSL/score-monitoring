#!/usr/bin/env python

"""
"""

import sys
from datetime import datetime
from dotenv import load_dotenv
import os
import pathlib
import subprocess

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from botocore.errorfactory import ClientError
import cartopy.crs as ccrs
from matplotlib import pyplot as plt
from netCDF4 import Dataset
import cftime
import numpy as np
import colorcet as cc

EARTH_RADIUS = 6.37 * 10**6 # meters

class SurfaceMapper(object):
    def __init__(self, input_cycle, input_env):
        """
        """
        load_dotenv(os.path.join(pathlib.Path(__file__).parent.parent.resolve(), input_env))

        self.parse_datetime(input_cycle)
        self.get_bucket()
        self.work_dir = os.getenv('CYLC_TASK_WORK_DIR')
        
        self.download_output_files()
        self.clean_output_files()

    def parse_datetime(self, input_cycle):
        """
        """
        self.datetime_obj = datetime.strptime(input_cycle, "%Y%m%dT%H")
        self.datetime_str = self.datetime_obj.strftime("%Y%m%d%H")
        self.cycle_str = self.datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    def get_bucket(self):
        """
        """
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
        
    def download_output_files(self):
        """
        """
        fv3atm_file_key = os.getenv('FV3ATM_FILE_KEY')
        fv3sfc_file_name = os.getenv('FV3SFC_FILE_NAME')
        fv3sfc_file_name1 = os.getenv('FV3SFC_FILE_NAME1')
        
        if fv3atm_file_key == '' or fv3atm_file_key == None:
            prefix = self.datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/")
        else:
            prefix = self.datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/" + fv3atm_file_key + "/")

        self.dest_file_path = list()
        for fv3sfc_file_idx, fv3sfc_file in enumerate([fv3sfc_file_name, fv3sfc_file_name1]):
            target_file_name = datetime.strftime(self.datetime_obj,
                                                 format = fv3sfc_file)
            self.dest_file_path.append(os.path.join(self.work_dir, target_file_name))
        
            try:
                self.bucket.download_file(prefix + target_file_name, self.dest_file_path[fv3sfc_file_idx])
            except ClientError as err:
                if err.response['Error']['Code'] == "404":
                    print(f"File {target_file_name} not found at {prefix}")
                    print(err)
                    raise err
                else:
                    print(err)
                    raise err
                
    def clean_output_files(self,
                           fv3atm_lon_var = 'grid_xt',
                           fv3atm_lat_var = 'grid_yt',
                           fv3atm_lw_var = 'ulwrf',
                           fv3atm_sw_var = 'uswrf',
                           ):
        self.lon_var = fv3atm_lon_var
        self.lat_var = fv3atm_lat_var
        self.lw_var = fv3atm_lw_var
        self.sw_var =fv3atm_sw_var
        
        self.rgr_file_path = list()
        for file_path_idx, file_path in enumerate(self.dest_file_path):
            base, ext = os.path.splitext(file_path)
            self.rgr_file_path.append(f"{base}_rgr{ext}")
            cmd = [
            "ncremap",
            "-v", f"{self.lw_var},{self.sw_var}", # variables
            "-R", "--rgr", f"lat_nm_in={self.lat_var}", "--rgr", f"lon_nm_in={self.lon_var}",
            "-d", f"{file_path}", # data file
            f"{file_path}", # input file
            f"{self.rgr_file_path[file_path_idx]}" # output file
            ]
            cmd = f"ncremap -v {self.lw_var},{self.sw_var} -R '--rgr lat_nm_in={self.lat_var} --rgr lon_nm_in={self.lon_var}' -d {file_path} {file_path} {self.rgr_file_path[file_path_idx]}"
 
            subprocess.run(cmd, check=True, shell=True)
            os.remove(file_path)
                
    def view_surface(self):
        for file_path in self.rgr_file_path:
            rootgrp = Dataset(file_path)
            time = rootgrp.variables['time']
            time_str = datetime.strftime(cftime.num2pydate(time[0],
                                                           units=time.units,
                                                           calendar=time.calendar),
                                         "%Y%m%dT%H")

            ax = plt.axes(projection=ccrs.Mercator(central_longitude=180.))
            
            lw_vals = (EARTH_RADIUS * rootgrp.variables['area'][:] *
                          rootgrp.variables[self.lw_var][0,:,:])
            sw_vals = (EARTH_RADIUS * rootgrp.variables['area'][:] *
                       rootgrp.variables[self.sw_var][0,:,:])
            lw_max_val = np.ma.max(lw_vals)
            lw_min_val = np.ma.min(lw_vals)

            ax.pcolormesh(rootgrp.variables[self.lon_var][:],
                          rootgrp.variables[self.lat_var][:],
                          np.ma.masked_where(sw_vals > 0, lw_vals),
                          cmap=cc.cm.CET_L8,
                          shading='nearest',
                          rasterized=True,
                          vmax=lw_max_val,
                          vmin=lw_min_val,
                          alpha=0.2,
                          zorder=3, edgecolors='face',
                          transform=ccrs.Mercator(central_longitude=180.,
                                                  min_latitude=-90.,
                                                  max_latitude=90.)
            )

            sw_max_val = np.ma.max(sw_vals)

            #sw_bright_vals = np.ma.masked_where(sw_vals < 0.1 * sw_max_val, sw_vals)
            sw_mid_vals = np.ma.masked_where(sw_vals > 0.10 * sw_max_val,
                                             sw_vals)
            #np.ma.masked_where(sw_mid_vals <= 0.01 * sw_max_val, sw_mid_vals, copy=False)
            sw_dark_vals = np.ma.masked_where(sw_vals > 0.001*sw_max_val, sw_vals)
            
            ax.pcolormesh(rootgrp.variables[self.lon_var][:],
                          rootgrp.variables[self.lat_var][:],
                          sw_dark_vals,
                          cmap=cc.cm.CET_L5,
                          shading='nearest',
                          rasterized=True, alpha = 0.2,
                          zorder=2, edgecolors='face',
                          transform=ccrs.Mercator(central_longitude=180.,
                                                  min_latitude=-90.,
                                                  max_latitude=90.)
            )
            ax.pcolormesh(rootgrp.variables[self.lon_var][:],
                          rootgrp.variables[self.lat_var][:],
                          sw_mid_vals,
                          cmap=cc.cm.CET_L15,
                          vmin=0,
                          vmax=0.44*sw_max_val,
                          shading='nearest',
                          rasterized=True,
                          zorder=1, edgecolors='face',
                          transform=ccrs.Mercator(central_longitude=180.,
                                                  min_latitude=-90.,
                                                  max_latitude=90.)
            )

            ax.pcolormesh(rootgrp.variables[self.lon_var][:],
                          rootgrp.variables[self.lat_var][:],
                          sw_vals,
                          cmap=cc.cm.CET_L1,
                          vmin=0,
                          shading='nearest',
                          rasterized=True,
                          zorder=0, edgecolors='face',
                          transform=ccrs.Mercator(central_longitude=180.,
                                                  min_latitude=-90.,
                                                  max_latitude=90.)
            )

            plt.savefig(os.path.join(self.work_dir, f'fv3sfc_{time_str}.png'),
                        dpi=300)
        
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
