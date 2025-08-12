#!/usr/bin/env python

"""Script to download FV3 surface data from an S3 bucket, process it,
optionally accumulate averages, and render surface radiation maps.
"""

import sys
import shutil
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
import matplotlib.ticker as mticker
from netCDF4 import Dataset
import cftime
import numpy as np
import colorcet as cc

EARTH_RADIUS = 6.37 * 10**6 # meters
SHARE_DATA_FILE = 'fv3sfc.nc'

FONTNAME = 'Noto Serif CJK JP'
FONTSIZE = 10
FONTCOLOR = 'black'

class SurfaceMapper(object):
    """Handles retrieval, processing, accumulation, and visualization of FV3 surface radiation data.
    """
    def __init__(self, input_cycle, input_env, integrate=True,
                 sw_exposure=0.7 # 0 to 1
                 ):
        """
        Initialize the SurfaceMapper.

        Args:
            input_cycle (str): Cycle time in YYYYMMDDTHH format.
            input_env (str): Path to .env file relative to script's parent directory.
            integrate (bool): Whether to accumulate running totals across cycles.
        """
        load_dotenv(os.path.join(pathlib.Path(__file__).parent.parent.resolve(), input_env))

        self.initial_cycle_point = os.getenv("CYLC_WORKFLOW_INITIAL_CYCLE_POINT")
        self.work_dir = os.getenv('CYLC_TASK_WORK_DIR')
        self.share_dir = os.getenv('CYLC_WORKFLOW_SHARE_DIR')
        self.integrate = integrate
        self.luminosity_scalar = 1 - sw_exposure
        
        self.parse_datetime(input_cycle)
        self.get_bucket()
        self.download_output_files()
        self.clean_output_files()
        if self.integrate:
            self.update_running_total_file(os.path.join(self.share_dir, SHARE_DATA_FILE),
                                           var_list=[self.sw_ave_var,
                                                     self.lw_ave_var,
                                                     self.lw_ave_var_cstoa,
                                                     self.sw_ave_var_cstoa,
                                                     self.lw_ave_var_toa,
                                                     self.sw_ave_var_toa],
                                           time_var='time')

    def parse_datetime(self, input_cycle):
        """Parse and store input cycle datetime information.
        """
        self.datetime_obj = datetime.strptime(input_cycle, "%Y%m%dT%H")
        self.datetime_str = self.datetime_obj.strftime("%Y%m%d%H")
        self.cycle_str = self.datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
        self.initial_cycle_point_datetime_obj = datetime.strptime(self.initial_cycle_point, "%Y%m%dT%H")
        self.initial_cycle_point_str = self.initial_cycle_point_datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    def get_bucket(self):
        """Initialize an S3 bucket resource using credentials from environment or unsigned access.
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
        """Download surface files (e.g. flux output) from the S3 bucket to the working directory.
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
                           fv3atm_sw_ave_var = 'uswrf_ave',
                           fv3atm_lw_ave_var = 'ulwrf_ave',
                           fv3atm_land_mask = 'land',
                           fv3cstoa_lw_ave_var = 'csulftoa',
                           fv3cstoa_sw_ave_var = 'csusftoa',
                           fv3toa_lw_ave_var = 'ulwrf_avetoa',
                           fv3toa_sw_ave_var = 'uswrf_avetoa',
                           ):
        """Regrid and clean downloaded NetCDF surface files using `ncremap`.

        Args:
            fv3atm_lon_var (str): Name of longitude variable.
            fv3atm_lat_var (str): Name of latitude variable.
            fv3atm_lw_var (str): Longwave flux variable.
            fv3atm_sw_var (str): Shortwave flux variable.
            fv3atm_sw_ave_var (str): Averaged shortwave flux variable.
            fv3atm_lw_ave_var (str): Averaged longwave flux variable.
        """
        self.lon_var = fv3atm_lon_var
        self.lat_var = fv3atm_lat_var
        self.lw_var = fv3atm_lw_var
        self.sw_var =fv3atm_sw_var
        self.lw_ave_var = fv3atm_lw_ave_var
        self.sw_ave_var =fv3atm_sw_ave_var
        self.land_mask = 'land'
        
        # top of atmosphere variables
        self.lw_ave_var_cstoa = fv3cstoa_lw_ave_var
        self.sw_ave_var_cstoa = fv3cstoa_sw_ave_var
        self.lw_ave_var_toa = fv3toa_lw_ave_var
        self.sw_ave_var_toa = fv3toa_sw_ave_var
        
        self.rgr_file_path = list()
        for file_path_idx, file_path in enumerate(self.dest_file_path):
            base, ext = os.path.splitext(file_path)
            self.rgr_file_path.append(f"{base}_rgr{ext}")
            if self.integrate:
                cmd = f"ncremap -v {self.lw_var},{self.sw_var},{self.lw_ave_var},{self.sw_ave_var},{self.land_mask},{self.lw_ave_var_cstoa},{self.sw_ave_var_cstoa},{self.lw_ave_var_toa},{self.sw_ave_var_toa} -R '--rgr lat_nm_in={self.lat_var} --rgr lon_nm_in={self.lon_var}' -d {file_path} {file_path} {self.rgr_file_path[file_path_idx]}"
            else:
                cmd = f"ncremap -v {self.lw_var},{self.sw_var},{self.land_mask} -R '--rgr lat_nm_in={self.lat_var} --rgr lon_nm_in={self.lon_var}' -d {file_path} {file_path} {self.rgr_file_path[file_path_idx]}"
 
            subprocess.run(cmd, check=True, shell=True)
            os.remove(file_path)

    def update_running_total_file(self, total_file_path, var_list, time_var='time'):
        """Accumulate specified variables over time into a persistent NetCDF file.

        Args:
            total_file_path (str): Path to persistent NetCDF output.
            var_list (list): List of variable names to accumulate.
            time_var (str): Name of the time variable in the NetCDF file.
        """
        for file_path in self.rgr_file_path:
            with Dataset(file_path) as ds:
                time = ds.variables[time_var]
                time_dt = cftime.num2pydate(time[0], units=time.units, calendar=time.calendar)

            if time_dt == self.initial_cycle_point_datetime_obj or not os.path.exists(total_file_path):
                shutil.copy(file_path, total_file_path)
            else:
                with Dataset(file_path) as src, Dataset(total_file_path, 'r+') as dst:
                    for var_name in var_list:
                        dst.variables[var_name][:] += src.variables[var_name][:]

    def map_surface(self, lon, lat, sw_vals, sw_max_val, sw_dark_vals, lw_vals):
        """Render a surface radiation plot with shortwave and longwave components.

        Args:
            lon (ndarray): Longitudes.
            lat (ndarray): Latitudes.
            sw_vals (ndarray): Shortwave flux values.
            sw_max_val (float): Maximum shortwave flux value for scaling.
            sw_dark_vals (ndarray): Masked shortwave values for highlighting.
            lw_vals (ndarray): Longwave flux values.
        """
        ax = plt.axes(projection=ccrs.Mercator(central_longitude=180.,
                                               #min_latitude=-70.,
                                               #max_latitude=70.
                                               )
        )

        gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='#A2A4A3', alpha=1.0, linestyle=':')
        gl.xlocator = mticker.FixedLocator(np.arange(-180, 181, 30))
        gl.ylocator = mticker.FixedLocator(np.arange(-90, 91, 15))
        gl.top_labels = False
        gl.right_labels = False
        gl.xlabel_style = {'fontname': FONTNAME, 'fontsize': FONTSIZE, 'color': FONTCOLOR}
        gl.ylabel_style = {'fontname': FONTNAME, 'fontsize': FONTSIZE, 'color': FONTCOLOR}

        ax.pcolormesh(lon,
                      lat,
                      sw_vals,
                      #cmap=cc.cm.CET_CBL3,
                      cmap=cc.cm.CET_L1,
                      vmin=0,
                      vmax=self.luminosity_scalar*sw_max_val,
                      shading='nearest',
                      rasterized=True,
                      zorder=0,
                      transform=ccrs.PlateCarree()
        )
        
        ax.pcolormesh(lon,
                      lat,
                      sw_dark_vals,
                      cmap=cc.cm.CET_L5,
                      #vmin=0,
                      #vmax=0.003333*sw_max_val,
                      shading='nearest',
                      rasterized=True,
                      zorder=2,
                      transform=ccrs.PlateCarree()
        )                                      
        ax.pcolormesh(lon,
                      lat,
                      np.ma.masked_where(sw_vals > 0, lw_vals),
                      cmap=cc.cm.CET_L8,
                      shading='nearest',
                      rasterized=True,
                      alpha=0.667,
                      zorder=3,
                      transform=ccrs.PlateCarree()
        )
        
        return ax
                                                              
    def view_surface(self):
        """Generate and save plots for each processed NetCDF file.
        """
        for file_path in self.rgr_file_path:
            rootgrp = Dataset(file_path)
            time = rootgrp.variables['time']
            time_dt = cftime.num2pydate(time[0],
                                        units=time.units,
                                        calendar=time.calendar)
            time_str = datetime.strftime(time_dt,
                                         "%Y%m%dT%H")
            time_label = datetime.strftime(time_dt,
                                           "%Y-%m-%d %H:%M:%S")
            
            lon = rootgrp.variables[self.lon_var][:]
            lat = rootgrp.variables[self.lat_var][:]
            
            land_mask = rootgrp.variables[self.land_mask][0,:,:]
            
            lw_vals = (EARTH_RADIUS * rootgrp.variables['area'][:] *
                          rootgrp.variables[self.lw_var][0,:,:])
            lw_max_val = np.ma.max(lw_vals)
            
            sw_vals = (EARTH_RADIUS * rootgrp.variables['area'][:] *
                       rootgrp.variables[self.sw_var][0,:,:])
                       
            sw_max_val = np.ma.max(sw_vals)
            
            sw_dark_vals = np.ma.masked_where(sw_vals > 0.0005 * sw_max_val,
                                              sw_vals)
            np.ma.masked_where(sw_dark_vals == 0, sw_dark_vals, copy=False)
        
            ax = self.map_surface(lon, lat, sw_vals, sw_max_val, sw_dark_vals, lw_vals)
            
            sw_vals_sea = np.ma.masked_where(land_mask != 0, sw_vals)
            np.ma.masked_where(sw_vals_sea < 0.05 * self.luminosity_scalar * sw_max_val,
                               sw_vals_sea, copy=False)
            np.ma.masked_where(sw_vals_sea > 0.95 * self.luminosity_scalar * sw_max_val,
                               sw_vals_sea, copy=False)
            
            ax.pcolormesh(lon,
                          lat,
                          sw_vals_sea,
                          cmap=cc.cm.CET_CBL3,
                          vmin=0.05*self.luminosity_scalar*sw_max_val,
                          vmax=0.95*self.luminosity_scalar*sw_max_val,
                          shading='nearest',
                          rasterized=True,
                          zorder=1,
                          transform=ccrs.PlateCarree()
            )
            '''
            sw_vals_land = np.ma.masked_where(land_mask != 1 ,sw_vals)
            np.ma.masked_where(sw_vals_land < 0.6 * self.luminosity_scalar * sw_max_val,
                               sw_vals_land, copy=False)
            np.ma.masked_where(sw_vals_land > 0.85 * self.luminosity_scalar * sw_max_val,
                               sw_vals_land, copy=False)
            ax.pcolormesh(lon,
                          lat,
                          sw_vals_land,
                          cmap=cc.cm.CET_L10,
                          vmin=0.6*self.luminosity_scalar*sw_max_val,
                          vmax=0.85*self.luminosity_scalar*sw_max_val,
                          shading='nearest',
                          rasterized=True,
                          zorder=1,
                          transform=ccrs.PlateCarree()
            )
            '''
            sw_vals_ice = np.ma.masked_where(land_mask != 2 ,sw_vals)
            np.ma.masked_where(sw_vals_ice < 0.05 * self.luminosity_scalar * sw_max_val,
                               sw_vals_ice, copy=False)
            np.ma.masked_where(sw_vals_ice > 0.95 * self.luminosity_scalar * sw_max_val,
                               sw_vals_ice, copy=False)
            
            ax.pcolormesh(lon,
                          lat,
                          sw_vals_ice,
                          cmap=cc.cm.CET_CBTL3,
                          vmin=0.05*self.luminosity_scalar*sw_max_val,
                          vmax=0.95*self.luminosity_scalar*sw_max_val,
                          shading='nearest',
                          rasterized=True,
                          zorder=1,
                          transform=ccrs.PlateCarree()
            )
            
            plt.title(time_label, fontsize=FONTSIZE, fontname=FONTNAME, color=FONTCOLOR)
            plt.savefig(os.path.join(self.work_dir, f'fv3sfc_{time_str}.png'),
                        dpi=300)
            plt.close()
        
            rootgrp.close()
            
        if self.integrate:
            self.view_surface_ave()
    
    def view_surface_ave(self):
        """Generate and save a plot of the accumulated average surface radiation.
        """
        rootgrp = Dataset(os.path.join(self.share_dir, SHARE_DATA_FILE))
        
        lon = rootgrp.variables[self.lon_var][:]
        lat = rootgrp.variables[self.lat_var][:]
        
        lw_ave_vals = (EARTH_RADIUS * rootgrp.variables['area'][:] *
                       rootgrp.variables[self.lw_ave_var][0,:,:])
        lw_ave_max_val = np.ma.max(lw_ave_vals)
        
        sw_ave_vals = (EARTH_RADIUS * rootgrp.variables['area'][:] *
                       rootgrp.variables[self.sw_ave_var][0,:,:])
        sw_ave_max_val = np.ma.max(sw_ave_vals)
        
        sw_ave_dark_vals = np.ma.masked_where(sw_ave_vals > 0.0005 * sw_ave_max_val,
                                              sw_ave_vals)
        np.ma.masked_where(sw_ave_dark_vals == 0, sw_ave_dark_vals, copy=False)
    
        self.map_surface(lon, lat, sw_ave_vals, sw_ave_max_val, sw_ave_dark_vals, lw_ave_vals)
        
        plt.title(f'start: {self.initial_cycle_point_str}; end: {self.cycle_str}',
                  fontsize=FONTSIZE, fontname=FONTNAME, color=FONTCOLOR)
        plt.savefig(os.path.join(self.share_dir, f'fv3sfc.png'),
                    dpi=300)
        plt.close()
    
        rootgrp.close()
    
    def view_toa_ave(self, clearsky=False, return_ax=False):
        """Generate and save a plot of the accumulated average TOA radiation.
        """
        rootgrp = Dataset(os.path.join(self.share_dir, SHARE_DATA_FILE))
        
        lon = rootgrp.variables[self.lon_var][:]
        lat = rootgrp.variables[self.lat_var][:]
        
        if clearsky:
            lw_varname = self.lw_ave_var_cstoa
            sw_varname = self.sw_ave_var_cstoa
            figname = 'fv3toa_cs.png'
        else:
            lw_varname = self.lw_ave_var_toa
            sw_varname = self.sw_ave_var_toa
            figname = 'fv3toa.png'
        
        lw_ave_vals = (EARTH_RADIUS * rootgrp.variables['area'][:] *
                       rootgrp.variables[lw_varname][0,:,:])
        lw_ave_max_val = np.ma.max(lw_ave_vals)
        
        sw_ave_vals = (EARTH_RADIUS * rootgrp.variables['area'][:] *
                       rootgrp.variables[sw_varname][0,:,:])
        sw_ave_max_val = np.ma.max(sw_ave_vals)
        
        sw_ave_dark_vals = np.ma.masked_where(sw_ave_vals > 0.0005 * sw_ave_max_val,
                                              sw_ave_vals)
        np.ma.masked_where(sw_ave_dark_vals == 0, sw_ave_dark_vals, copy=False)
    
        ax = self.map_surface(lon, lat, sw_ave_vals, sw_ave_max_val, sw_ave_dark_vals, lw_ave_vals)
        
        rootgrp.close()
            
        if return_ax:
            return ax
        else:
            plt.title(f'start: {self.initial_cycle_point_str}; end: {self.cycle_str}',
                  fontsize=FONTSIZE, fontname=FONTNAME, color=FONTCOLOR)
            plt.savefig(os.path.join(self.share_dir, figname),
                        dpi=300)
            plt.close()
        
    def map_soca_obs(self, soca_obs_dir='store_data_soca_obsfit', max_size=100):
        """
        """
        soca_obs_path = pathlib.Path(self.work_dir).parent / soca_obs_dir
        soca_diag_files = list(soca_obs_path.glob('*.nc')) + list(soca_obs_path.glob('*.nc4'))
        
        ax = self.view_toa_ave(clearsky=True, return_ax=True)
        soca_obs_exist = False
        for soca_diag_file in soca_diag_files:
            rootgrp = Dataset(soca_diag_file)
            meta_grp = rootgrp.groups['MetaData']
            ombg_grp = rootgrp.groups['ombg']
            obsvalue_grp = rootgrp.groups['ObsValue']
            
            lats = meta_grp.variables['latitude'][:]
            lons = meta_grp.variables['longitude'][:]
            depths = meta_grp.variables['depth'][:]
            for var, ombg in ombg_grp.variables.items():
                
                if var=='waterTemperature':
                    marker='o'
                    obsvals = obsvalue_grp[var][:] + 273.15
                else:
                    marker = '+'
                    obsvals = obsvalue_grp[var][:]
                    
                relative_errs = (-1 * ombg[:]) / obsvals
                
                sc = ax.scatter(x=lons, y=lats, c=relative_errs, edgecolors=FONTCOLOR,
                               label=var, vmin=-0.025, vmax=0.025,
                               s=np.clip(max_size / (depths + 0.01), 5, max_size),
                               alpha=0.9,
                               transform=ccrs.PlateCarree(),
                               zorder=4,
                               cmap=cc.cm.CET_D9)
                soca_obs_exist = True

            rootgrp.close()
        
        if soca_obs_exist:
            ax.legend(loc='lower left')
            cbar = plt.colorbar(sc, ax=ax, orientation='vertical')
            cbar.ax.tick_params(labelsize=FONTSIZE, labelcolor=FONTCOLOR)
            for label in cbar.ax.get_yticklabels():
                label.set_fontname(FONTNAME)
            cbar.set_label('relative error', fontsize=FONTSIZE, fontname=FONTNAME, color=FONTCOLOR)
            plt.title(self.cycle_str, fontsize=FONTSIZE, fontname=FONTNAME, color=FONTCOLOR)
            plt.savefig(os.path.join(self.work_dir, f'gdas_analysis_ocean_diags.png'),
                    dpi=300)

        plt.close()
        
def run():
    """Run the SurfaceMapper with command-line arguments.
    """
    surface_mapper = SurfaceMapper(sys.argv[1], sys.argv[2])
    surface_mapper.view_surface()
    surface_mapper.view_toa_ave()
    surface_mapper.view_toa_ave(clearsky=True)
    surface_mapper.map_soca_obs()

def main():
    """Main entry point.
    """
    run()

if __name__=='__main__':
    main()
