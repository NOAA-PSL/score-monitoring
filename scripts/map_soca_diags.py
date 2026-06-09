#!/usr/bin/env python

"""Script to download data from an S3 bucket, process it and render  maps.
"""

import sys
from datetime import datetime
from dotenv import load_dotenv
import os
import pathlib
import pandas as pd

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
from matplotlib.colors import BoundaryNorm

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

    # def download_output_files(self, soca_diags_file_names):
    def download_output_files(self, soca_diags_file_names=None):
        """Download files from the S3 bucket to the working directory.
        """
        soca_diags_key = os.getenv('SOCA_DIAGS_KEY') 
        print('hello world! - need to download files')

        if soca_diags_key == '' or soca_diags_key == None:
            prefix = self.datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/")
        else:
            prefix = self.datetime_obj.strftime(os.getenv('STORAGE_LOCATION_KEY') + "/" + soca_diags_key + "/")

        self.dest_file_path = list()
        
        # -----------------------------
        # NEW: auto-discover files if not provided
        # -----------------------------
        if soca_diags_file_names is None:
            soca_diags_file_names = [
                os.path.basename(obj.key)
                for obj in self.bucket.objects.filter(Prefix=prefix)
                if obj.key.endswith((".nc", ".nc4"))
            ]

            soca_diags_file_names.sort()

        self.dest_file_path = []

        for target_file_name_idx, target_file_name in enumerate(soca_diags_file_names):
            dest_path = os.path.join(self.work_dir, target_file_name)
            self.dest_file_path.append(dest_path)

            try:
                self.bucket.download_file(prefix + target_file_name, dest_path)

            except ClientError as err:
                error_code = err.response["Error"]["Code"]

                if error_code in ["404", "NoSuchKey", "NotFound"]:
                    print(f"WARNING: {target_file_name} not found at {prefix}")
                    continue

                print(err)
                raise err

    def map_soca_obs(self, soca_obs_dir='soca_diags_mapper', max_size=100):
        """ figure mapper function
        """
        soca_obs_path = pathlib.Path(self.work_dir).parent / soca_obs_dir
        soca_diag_files = list(soca_obs_path.glob('*.nc')) + list(soca_obs_path.glob('*.nc4'))
        
        ## parameters for depth binning and colorbar limits
        depth_bins = [
            ("1–10 m", (1,10)),
            ("10–100 m", (10,100)),
            ("100–500 m", (100,500)),
            ("500+ m", (500,np.inf)),
        ]
        sfc_colorbar_limits = {
            "seaIceFraction": 0.5,
            "absoluteDynamicTopography": 0.5,
            "seaSurfaceTemperature": 1.0,
        }
        depth_T_colorbar_limits = {
            "mean": 1,
            "std": 0.5,
        }
        depth_s_colorbar_limits = {
            "mean": 0.5,
            "std": 0.25,
        }
        ##### functions 

        def determine_hemisphere(lat):
            lat_mean = np.nanmean(lat)
            if lat_mean >= 0:
                return "NH"
            else:
                return "SH"
            
        def size_from_n(n):
            return 5 + 3 * np.sqrt(n)
        #####  

        
        soca_obs_exist = bool(soca_diag_files)
        if not soca_obs_exist:
            print(f"No SOCA diagnostic files found in {soca_obs_path}")
            return  # or skip processing
        
        ## loop through files
        for soca_diag_file in soca_diag_files:
            print(soca_diag_file.name)
            rootgrp = Dataset(soca_diag_file)
            meta_grp = rootgrp.groups['MetaData']
            ombg_grp = rootgrp.groups['ombg']
            # obsvalue_grp = rootgrp.groups['ObsValue']
            effectiveQC0_grp = rootgrp.groups['EffectiveQC0']

            lats = meta_grp.variables['latitude'][:]
            lons = meta_grp.variables['longitude'][:]
            has_depth = 'depth' in meta_grp.variables

            if not has_depth:
                print(f"{soca_diag_file.name}: no depth variable → plotting surface map")
    
            depths = meta_grp.variables['depth'][:] if has_depth else None

            ## loop through variables in ombg group 
            valid_geo = np.isfinite(lons) & np.isfinite(lats)
            for var, ombg in ombg_grp.variables.items():
                print(var)
                # obsvals = obsvalue_grp[var][:] + 273.15 if var=='waterTemperature' else obsvalue_grp[var][:]
                ombg_vals = ombg[:]
                effQC0_vals = effectiveQC0_grp[var][:]
                ombg_arr = np.where(effQC0_vals==0, ombg_vals, np.nan)
                
                print(var, "RAW min/max:", np.nanmin(ombg_vals[valid_geo]), np.nanmax(ombg_vals[valid_geo]))
                print(var, "QC min/max:", np.nanmin(ombg_arr[valid_geo]), np.nanmax(ombg_arr[valid_geo]))
                # relative_errs = (-1 * ombg[:]) / obsvals

                # =========================================================
                # CASE 1: NO DEPTH → SIMPLE SCATTER
                # =========================================================
                if not has_depth:
                    ## determine projection based on variable type and hemisphere 
                    if var == 'seaIceFraction':
                        hemisphere = determine_hemisphere(lats[valid_geo])
                        if hemisphere == "NH":

                            fig, ax = plt.subplots(
                                1, 1,
                                figsize=(9, 4),
                                subplot_kw={"projection": ccrs.NorthPolarStereo()},
                                constrained_layout=True
                            )
                        elif hemisphere == "SH":
                            fig, ax = plt.subplots(
                                1, 1,
                                figsize=(9, 4),
                                subplot_kw={"projection": ccrs.SouthPolarStereo()},
                                constrained_layout=True
                            )
                    else:
                        fig, ax = plt.subplots(
                            1, 1,
                            figsize=(9, 4),
                            subplot_kw={"projection": ccrs.PlateCarree(central_longitude=200)},
                            constrained_layout=True
                        )

                ## plotting parameters and styling
                    markersize=2
                    lon_grid_ints=60
                    lat_grid_ints=30            
                    if var == 'seaIceFraction':
                        if hemisphere == "NH":
                            ax.set_extent([-180, 180, 45, 90], ccrs.PlateCarree())
                        elif hemisphere == "SH":
                            ax.set_extent([-180, 180, -90, -45], ccrs.PlateCarree())
                    else:
                        ax.set_global()

                    ax.coastlines(resolution='110m', linewidth=0.8)
                    ax.add_feature(cfeature.LAND, facecolor='lightgray')
                    ax.add_feature(cfeature.OCEAN, facecolor='white')
                    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='#A2A4A3', alpha=1.0, linestyle=':', zorder=10)
                    gl.xlocator = mticker.FixedLocator(np.arange(-180, 181, lon_grid_ints))
                    gl.ylocator = mticker.FixedLocator(np.arange(-90+lat_grid_ints, 90, lat_grid_ints))
                    gl.top_labels = False
                    gl.right_labels = False
                    gl.xlabel_style = {'fontname': FONTNAME, 'fontsize': FONTSIZE, 'color': FONTCOLOR}
                    gl.ylabel_style = {'fontname': FONTNAME, 'fontsize': FONTSIZE, 'color': FONTCOLOR}

                    if var in sfc_colorbar_limits:
                        limit = sfc_colorbar_limits[var]
                    else:
                        print('the var is not in the predefined colorbar limits list, using automatic limit')
                        limit = np.percentile(np.abs(ombg_arr[valid_geo]), 95)

                    nlevels = 21  # odd number so 0 sits in center
                    bounds = np.linspace(-limit, limit, nlevels)
                    norm = BoundaryNorm(
                        bounds,
                        ncolors=cc.cm.CET_D9.N,
                        clip=False
                    )

                    sc = ax.scatter(
                        lons[valid_geo],
                        lats[valid_geo],
                        c=ombg_arr[valid_geo],
                        s=markersize,
                        cmap=cc.cm.CET_D9,
                        norm=norm,
                        transform=ccrs.PlateCarree()
                    )
                    ## put the max and min values in the title for quick reference (after QC filtering)
                    vmin=np.nanmin(ombg_arr[valid_geo])
                    vmax=np.nanmax(ombg_arr[valid_geo])
                    base_name = soca_diag_file.stem
                    ax.set_title(f"{base_name}, {self.datetime_str} (max: {vmax:.2f}, min: {vmin:.2f}), OmB")
                    fig.colorbar(sc, ax=ax, orientation="horizontal",
                                shrink=1.1,   # make it longer (default is 1.0)
                                pad=0.08, fraction=0.05).set_label("Obs − Background")

                    outfile = os.path.join(
                        self.work_dir,
                        f"{base_name}_{self.datetime_str}_sfc.png"
                    )

                    fig.savefig(outfile, dpi=300, bbox_inches="tight")
                    plt.close(fig)

                    continue  # IMPORTANT: skip depth logic (things below are skipped if no depth variable)
                # =========================================================
                # CASE 2: DEPTH EXISTS → BIN + MEAN/STD
                # =========================================================
                else:
                    fig_mean, axes_mean = plt.subplots(
                        2, 2,
                        figsize=(11, 6),
                        subplot_kw={"projection": ccrs.PlateCarree(central_longitude=200)}
                    )
                    fig_mean.subplots_adjust(
                        wspace=0.001,  # smaller → columns closer
                        hspace=0.2    # larger → rows farther apart
                    )
                    fig_std, axes_std = plt.subplots(
                        2, 2,
                        figsize=(11, 6),
                        subplot_kw={"projection": ccrs.PlateCarree(central_longitude=200)}
                    )
                    fig_std.subplots_adjust(
                        wspace=0.001,  # smaller → columns closer
                        hspace=0.2    # larger → rows farther apart
                    )

                    fig_mean.subplots_adjust(right=0.85)
                    fig_std.subplots_adjust(right=0.85)

                    axes_mean = axes_mean.flatten()
                    axes_std = axes_std.flatten()

                    has_any_depth_data = False


                    all_mean_max_vals = []
                    all_std_max_vals = []
                    depth_results = []

                    for (label, (zmin, zmax)) in depth_bins:

                        # -------------------------
                        # depth mask
                        # -------------------------
                        depth_mask = (
                            (depths >= zmin) &
                            (depths < zmax) &
                            valid_geo
                        )

                        if not np.any(depth_mask):
                            depth_results.append((label, None))
                            print(f"{soca_diag_file.name}, {var}, {label}: no valid data in this depth range → skipping this subplot")
                            continue
                        has_any_depth_data = True


                        df = pd.DataFrame({
                            "lon": lons[depth_mask],
                            "lat": lats[depth_mask],
                            "ombg": ombg_arr[depth_mask]
                        })

                        ## calculate mean and std
                        agg = df.groupby(["lon", "lat"]).agg(
                            mean_ombg=("ombg", "mean"),
                            std_ombg=("ombg", "std"),
                            nobs=("ombg", "size")
                        ).reset_index()

                        ### save the agg results for this depth bin to use in plotting loop below,
                        # and also to calculate global colorbar limits across all subplots
                        depth_results.append((label, agg))
                        
                        if agg["mean_ombg"].notna().any():
                            all_mean_max_vals.append(np.nanmax(np.abs(agg["mean_ombg"])))
                        if agg["std_ombg"].notna().any():
                            all_std_max_vals.append(np.nanmax(agg["std_ombg"]))


                    
                    ### before plotting 4 subplots
                    ### calculated the shared plot colorbar limits based on global min/max across subplots
                    if len(all_mean_max_vals) == 0 or len(all_std_max_vals) == 0:
                        print(f"{soca_diag_file.name}, {var}: no depth data in any bin")
                        plt.close(fig_mean)
                        plt.close(fig_std)
                        continue

                    if var == 'waterTemperature':
                        limit_mean = depth_T_colorbar_limits['mean']
                        limit_std = depth_T_colorbar_limits['std']
                    elif var == 'salinity':
                        limit_mean = depth_s_colorbar_limits['mean']
                        limit_std = depth_s_colorbar_limits['std']
                    else:
                        print('Depth figs: the var is not in the predefined waterTemperature limits list, using automatic limit')
                        limit_mean = np.percentile(np.abs(all_mean_max_vals), 95)
                        limit_std = np.percentile(np.abs(all_std_max_vals), 95)
                        
                    nlevels = 21
                    bound_mean = np.linspace(-limit_mean, limit_mean, nlevels)
                    norm_mean = BoundaryNorm(
                        bound_mean,
                        ncolors=cc.cm.CET_D9.N,
                        clip=False
                    )
                    
                    bound_std = np.linspace(0, limit_std, nlevels)
                    norm_std = BoundaryNorm(
                        bound_std,
                        ncolors=cc.cm.CET_D9.N,
                        clip=False
                    )
                    ## plotting loop for each depth bin subplot
                    for i, ((ax_mean, ax_std), (label, agg)) in enumerate(zip(zip(axes_mean, axes_std), depth_results)):
                        if agg is None:
                            ax_mean.set_title(f"{label} (no data)")
                            ax_std.set_title(f"{label} (no data)")
                            continue

                        col = i % 2
                        row = i // 2
                        # -------------------------
                        # map styling
                        # -------------------------
                        for ax in [ax_mean, ax_std]:
                            lon_grid_ints = 60
                            lat_grid_ints = 30
                            ax.set_global()
                            ax.coastlines()
                            ax.add_feature(cfeature.LAND, facecolor='lightgray')
                            ax.add_feature(cfeature.OCEAN, facecolor='white')
                            gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='#A2A4A3', alpha=1.0, linestyle=':', zorder=10)
                            gl.xlocator = mticker.FixedLocator(np.arange(-180, 181, lon_grid_ints))
                            gl.ylocator = mticker.FixedLocator(np.arange(-90+lat_grid_ints, 90, lat_grid_ints))
                            gl.top_labels = False
                            gl.right_labels = False
                            # remove duplicate labels
                            if col == 1:
                                gl.left_labels = False      # no latitude labels on right column
                            if row == 0:
                                gl.bottom_labels = False    # no longitude labels on top row

                            gl.xlabel_style = {'fontname': FONTNAME, 'fontsize': FONTSIZE, 'color': FONTCOLOR}
                            gl.ylabel_style = {'fontname': FONTNAME, 'fontsize': FONTSIZE, 'color': FONTCOLOR}


                        # -------------------------
                        # MEAN PLOT
                        # -------------------------
                        sc_mean = ax_mean.scatter(
                            agg["lon"], agg["lat"],
                            c=agg["mean_ombg"],
                            s=markersize,
                            cmap=cc.cm.CET_D9,
                            norm=norm_mean,
                            transform=ccrs.PlateCarree()
                        )
                        if agg["mean_ombg"].notna().any():
                            vmin_mean=np.nanmin(agg["mean_ombg"])
                            vmax_mean=np.nanmax(agg["mean_ombg"])
                        else:
                            vmin_mean, vmax_mean = np.nan, np.nan
                        ax_mean.set_title(label + f" (max: {vmax_mean:.2f}, min: {vmin_mean:.2f})")

                        # -------------------------
                        # STD PLOT
                        # -------------------------
                        sc_std = ax_std.scatter(
                            agg["lon"], agg["lat"],
                            c=agg["std_ombg"],
                            s=size_from_n(agg["nobs"]),
                            cmap="viridis",
                            norm=norm_std,
                            transform=ccrs.PlateCarree()
                        )
                        if agg["std_ombg"].notna().any():
                            vmin_std = np.nanmin(agg["std_ombg"])
                            vmax_std = np.nanmax(agg["std_ombg"])
                        else:
                            vmin_std, vmax_std = np.nan, np.nan
                        ax_std.set_title(label + f" (max: {vmax_std:.2f}, min: {vmin_std:.2f})")

                    legend_vals = [5, 10, 25, 50, 100, 250, 500, 750, 1000]
                    handles = [
                        ax_mean.scatter([], [], s=size_from_n(v),
                                        color="gray", alpha=0.6,
                                        transform=ccrs.PlateCarree())
                        for v in legend_vals
                    ]

                
                    # =========================================================
                    # SAVE OUTPUTS
                    # =========================================================

                    base_name = soca_diag_file.stem
                    if not has_any_depth_data:
                        print(f"{soca_diag_file.name}: no valid depth data")
                        plt.close(fig_mean)
                        plt.close(fig_std)
                        continue

                    fig_mean.colorbar(sc_mean, ax=axes_mean,
                                    orientation="horizontal",
                                    shrink=1.1,   #  make it longer (default is 1.0)
                                    pad=0.05, fraction=0.05).set_label("Mean (Obs − Background)")

                    fig_std.colorbar(sc_std, ax=axes_std,
                                    orientation="horizontal",
                                    shrink=1.1,   # make it longer (default is 1.0)
                                    pad=0.05, fraction=0.05).set_label("Std (Obs − Background)")

                    fig_mean.suptitle(f"{var}, {base_name}, {self.datetime_str}, Mean OmB")
                    fig_std.suptitle(f"{var}, {base_name}, {self.datetime_str}, Std OmB")


                    fig_mean.savefig(
                        os.path.join(self.work_dir, f"{base_name}_{self.datetime_str}_Depth_mean.png"),
                        dpi=300, bbox_inches="tight"
                    )

                    fig_std.savefig(
                        os.path.join(self.work_dir, f"{base_name}_{self.datetime_str}_Depth_std.png"),
                        dpi=300, bbox_inches="tight"
                    )

                    plt.close(fig_mean)
                    plt.close(fig_std)
                rootgrp.close()        

                
def run():
    """Run the SurfaceMapper with command-line arguments.
    """
    surface_mapper = SurfaceMapper(sys.argv[1], sys.argv[2])
    
    # for plotting all diags downloaded during score-hv/score-db harvesting
    # surface_mapper.map_soca_obs(soca_obs_dir='store_data_soca_obsfit')
    
    surface_mapper.download_output_files()
    surface_mapper.map_soca_obs(soca_obs_dir='soca_diags_mapper')

def main():
    """Main entry point.
    """
    run()

if __name__=='__main__':
    main()