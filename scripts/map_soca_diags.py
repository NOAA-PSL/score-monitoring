#!/usr/bin/env python

"""Script to download data from an S3 bucket, process it and render  maps.
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
import matplotlib.ticker as mticker
from netCDF4 import Dataset
import numpy as np
import cartopy.feature as cfeature

import colorcet as cc

NETCDF_FILL_VALUE = 9.969209968386869e+36

MEAN_SLP = 101325 # Pa
EARTH_RADIUS = 6.37 * 10**6 # meters

FONTNAME = 'Noto Serif CJK JP'
FONTSIZE = 10
FONTCOLOR = 'black'

class SurfaceMapper(object):
    """Handles retrieval, processing, and visualization of SOCA diags.
    """
    def __init__(self, input_cycle, input_env):
        """
        Args:
            input_cycle (str): Cycle time in YYYYMMDDTHH format.
            input_env (str): Path to .env file relative to script's parent directory.
            integrate (bool): Whether to accumulate running totals across cycles.
        """
        load_dotenv(os.path.join(pathlib.Path(__file__).parent.parent.resolve(), input_env))

        self.initial_cycle_point = os.getenv("CYLC_WORKFLOW_INITIAL_CYCLE_POINT")
        self.work_dir = os.getenv('CYLC_TASK_WORK_DIR')
        self.share_dir = os.getenv('CYLC_WORKFLOW_SHARE_DIR')
        
        self.parse_datetime(input_cycle)
        self.get_bucket()

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

    def download_output_files(self, soca_diags_file_names):
        """Download files from the S3 bucket to the working directory.
        """
        soca_diags_key = os.getenv('SOCA_DIAGS_KEY') 
        print('hello world! - need to download files')

        if soca_diags_key == '' or soca_diags_key == None:
            prefix = self.datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/")
        else:
            prefix = self.datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/" + soca_diags_key + "/")

        self.dest_file_path = list()
        
        for target_file_name_idx, target_file_name in enumerate(soca_diags_file_names):
            self.dest_file_path.append(os.path.join(self.work_dir, target_file_name))
            try:
                self.bucket.download_file(prefix + target_file_name, self.dest_file_path[target_file_name_idx])
            except ClientError as err:
                if err.response['Error']['Code'] == "404":
                    print(f"File {target_file_name} not found at {prefix}")
                    print(err)
                    raise err
                else:
                    print(err)
                    raise err

    def map_soca_obs(self, soca_obs_dir='soca_diags_mapper', max_size=100):
        """ figure mapper function
        """
        soca_obs_path = pathlib.Path(self.work_dir).parent / soca_obs_dir
        soca_diag_files = list(soca_obs_path.glob('*.nc')) + list(soca_obs_path.glob('*.nc4'))
        
        ax = plt.axes(projection=ccrs.Mercator(central_longitude=180.))
        ax.coastlines(resolution='110m', linewidth=0.8)
        ax.add_feature(cfeature.LAND, facecolor='lightgray', zorder=0)
        ax.add_feature(cfeature.OCEAN, facecolor='white', zorder=0)
        lon_grid_ints = 30
        lat_grid_ints = 15
        gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='#A2A4A3', alpha=1.0, linestyle=':', zorder=10)
        gl.xlocator = mticker.FixedLocator(np.arange(-180, 181, lon_grid_ints))
        gl.ylocator = mticker.FixedLocator(np.arange(-90+lat_grid_ints, 90, lat_grid_ints))
        gl.top_labels = False
        gl.right_labels = False
        gl.xlabel_style = {'fontname': FONTNAME, 'fontsize': FONTSIZE, 'color': FONTCOLOR}
        gl.ylabel_style = {'fontname': FONTNAME, 'fontsize': FONTSIZE, 'color': FONTCOLOR}
        
        depth_bins = {
            "1–10 m":   (1, 10),
            "10–100 m": (10, 100),
            "100+ m":   (100, np.inf),
        }

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
                
                fig, axes = plt.subplots(
                    nrows=1, ncols=3,
                    figsize=(18, 6),
                    subplot_kw={"projection": ccrs.PlateCarree()},
                    constrained_layout=True
                )
                # sc = ax.scatter(x=lons, y=lats, c=relative_errs, edgecolors=FONTCOLOR,
                #                label=var, vmin=-0.025, vmax=0.025,
                #                s=np.clip(max_size / (depths + 0.01), 5, max_size),
                #                alpha=0.9,
                #                transform=ccrs.PlateCarree(),
                #                zorder=4,
                #                cmap=cc.cm.CET_D9)

                ombg_arr = ombg[:]

                sc = None
                for ax, (label, (zmin, zmax)) in zip(axes, depth_bins.items()):

                    ax.set_global()

                    # Coastlines + land
                    ax.coastlines(resolution='110m', linewidth=0.8)
                    ax.add_feature(cfeature.LAND, facecolor='lightgray', zorder=0)
                    ax.add_feature(cfeature.OCEAN, facecolor='white', zorder=0)

                    depth_mask = (
                        (depths >= zmin) &
                        (depths < zmax) &
                        np.isfinite(ombg_arr) &
                        np.isfinite(lons) &
                        np.isfinite(lats)
                    )

                    if not np.any(depth_mask):
                        ax.set_title(f"{label}\n(no data)")
                        continue

                    sc = ax.scatter(
                        lons[depth_mask],
                        lats[depth_mask],
                        c=ombg_arr[depth_mask],
                        s=10,
                        alpha=0.9,
                        vmin=-0.025,
                        vmax=0.025,
                        cmap=cc.cm.CET_D9,
                        transform=ccrs.PlateCarree(),
                        zorder=4
                    )

                    ax.set_title(label)
                    soca_obs_exist = True

                if sc is not None:
                    cbar = fig.colorbar(
                        sc,
                        ax=axes,
                        orientation='horizontal',
                        pad=0.08,
                        fraction=0.05
                    )
                    cbar.set_label('Obs − Background')

                fig.suptitle(var)

                outfile = os.path.join(
                    self.work_dir,
                    f"gdas_analysis_ocean_diags_{var}_3depths.png"
                )
                plt.savefig(outfile, dpi=300, bbox_inches='tight')
                plt.close(fig)

            rootgrp.close()        
        # if soca_obs_exist:
        #     ax.legend(loc='lower left', fontsize=FONTSIZE, prop={"family":FONTNAME})
        #     cbar = plt.colorbar(sc, ax=ax, orientation='horizontal')
        #     cbar.ax.tick_params(labelsize=FONTSIZE, labelcolor=FONTCOLOR, labelfontfamily=FONTNAME)
        #     for label in cbar.ax.get_yticklabels():
        #         label.set_fontname(FONTNAME)
        #     cbar.set_label('relative error', fontsize=FONTSIZE, fontname=FONTNAME, color=FONTCOLOR)
        #     plt.title(self.cycle_str, fontsize=FONTSIZE, fontname=FONTNAME, color=FONTCOLOR)
        #     plt.savefig(os.path.join(self.work_dir, f'gdas_analysis_ocean_diags.png'),
        #             dpi=300)

        # plt.close()
                
def run():
    """Run the SurfaceMapper with command-line arguments.
    """
    surface_mapper = SurfaceMapper(sys.argv[1], sys.argv[2])
    
    # for plotting all diags downloaded during score-hv/score-db harvesting
    #surface_mapper.map_soca_obs(soca_obs_dir='store_data_soca_obsfit')
    
    # for plotting diags associated with specific output
    surface_mapper.download_output_files(['wod_t_xbt.nc',
                                          #'sst_avhrr_n20_l3u.nc',
                                          #'icec_amsr2_north',
                                          #'icec_amsr2_south',
                                          #'...'
                                          ])
    surface_mapper.map_soca_obs(soca_obs_dir='soca_diags_mapper')

def main():
    """Main entry point.
    """
    run()

if __name__=='__main__':
    main()