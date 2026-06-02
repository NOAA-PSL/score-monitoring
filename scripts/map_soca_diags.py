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
        

        depth_bins = [
            ("1–10 m", (1,10)),
            ("10–100 m", (10,100)),
            ("100–500 m", (100,500)),
            ("500+ m", (500,np.inf)),
        ]
        def nice_limit(x):
            exp = np.floor(np.log10(x))
            frac = x / 10**exp

            if frac <= 1:
                nice = 1
            elif frac <= 2:
                nice = 2
            elif frac <= 5:
                nice = 5
            else:
                nice = 10

            return nice * 10**exp

                
        soca_obs_exist = bool(soca_diag_files)
        if not soca_obs_exist:
            print(f"No SOCA diagnostic files found in {soca_obs_path}")
            return  # or skip processing
        
        for soca_diag_file in soca_diag_files:
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

            valid_geo = np.isfinite(lons) & np.isfinite(lats)
            for var, ombg in ombg_grp.variables.items():
                
                # obsvals = obsvalue_grp[var][:] + 273.15 if var=='waterTemperature' else obsvalue_grp[var][:]
                ombg_vals = ombg[:]
                effQC0_vals = effectiveQC0_grp[var][:]
                ombg_arr = np.where(effQC0_vals==0, ombg_vals, np.nan)

                # relative_errs = (-1 * ombg[:]) / obsvals

                # =========================================================
                # CASE 1: NO DEPTH → SIMPLE SCATTER
                # =========================================================
                if not has_depth:
                    markersize=2
                    fig, ax = plt.subplots(
                        1, 1,
                        figsize=(9, 4),
                        subplot_kw={"projection": ccrs.PlateCarree()},
                        constrained_layout=True
                    )
                    lon_grid_ints=60
                    lat_grid_ints=30
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

                    global_abs_max = np.nanmax(np.abs(ombg_arr[valid_geo]))
                    limit = nice_limit(global_abs_max * 0.5)

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
                    vmin=np.nanmin(ombg_arr[valid_geo])
                    vmax=np.nanmax(ombg_arr[valid_geo])
                    ax.set_title(f"{var} ,sfc (max: {vmax:.2f}, min: {vmin:.2f}), OmB")


                    base_name = soca_diag_file.stem
                    outfile = os.path.join(
                        self.work_dir,
                        f"{base_name}_{self.datetime_str}_sfc.png"
                    )

                    fig.colorbar(sc, ax=ax, orientation="horizontal",
                                shrink=1.1,   # make it longer (default is 1.0)
                                pad=0.08, fraction=0.05).set_label("Obs − Background")

                    fig.savefig(outfile, dpi=300, bbox_inches="tight")
                    plt.close(fig)

                    continue  # IMPORTANT: skip depth logic (things below are skipped if no depth variable)
            # =========================================================
            # CASE 2: DEPTH EXISTS → BIN + MEAN/STD
            # =========================================================

                fig_mean, axes_mean = plt.subplots(
                    2, 2,
                    figsize=(11, 6),
                    subplot_kw={"projection": ccrs.PlateCarree()}
                )
                fig_mean.subplots_adjust(
                    wspace=0.001,  # smaller → columns closer
                    hspace=0.2    # larger → rows farther apart
                )
                fig_std, axes_std = plt.subplots(
                    2, 2,
                    figsize=(11, 6),
                    subplot_kw={"projection": ccrs.PlateCarree()}
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

                def size_from_n(n):
                    return 5 + 3 * np.sqrt(n)


                
                all_mean_max_vals = []
                all_std_max_vals = []
                depth_results = []

                for (label, (zmin, zmax)) in depth_bins:
                    ### calculate the metrics and global min and max 
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

                    # -------------------------
                    # bin lat/lon
                    # -------------------------
                    lon_bin = 2.0
                    lat_bin = 2.0

                    lon_binned = np.round(lons[depth_mask] / lon_bin) * lon_bin
                    lat_binned = np.round(lats[depth_mask] / lat_bin) * lat_bin

                    df = pd.DataFrame({
                        "lon": lon_binned,
                        "lat": lat_binned,
                        "ombg": ombg_arr[depth_mask]
                    })

                    agg = df.groupby(["lon", "lat"]).agg(
                        mean_ombg=("ombg", "mean"),
                        std_ombg=("ombg", "std"),
                        nobs=("ombg", "size")
                    ).reset_index()

                    depth_results.append((label, agg))
                    
                    if agg["mean_ombg"].notna().any():
                        all_mean_max_vals.append(np.nanmax(np.abs(agg["mean_ombg"])))
                    if agg["std_ombg"].notna().any():
                        all_std_max_vals.append(np.nanmax(agg["std_ombg"]))


                ### build share plot colorbar limits based on global min/max across subplots
                ### before plotting 4 subplots
                if len(all_mean_max_vals) == 0 or len(all_std_max_vals) == 0:
                    print(f"{soca_diag_file.name}, {var}: no depth data in any bin")
                    plt.close(fig_mean)
                    plt.close(fig_std)
                    continue
                global_mean_limit = np.nanmax(all_mean_max_vals)
                global_std_limit = np.nanmax(all_std_max_vals)


                nlevels = 21
                limit_mean = nice_limit(global_mean_limit * 0.5)
                bound_mean = np.linspace(-limit_mean, limit_mean, nlevels)
                norm_mean = BoundaryNorm(
                    bound_mean,
                    ncolors=cc.cm.CET_D9.N,
                    clip=False
                )
                
                limit_std = nice_limit(global_std_limit * 0.5)
                bound_std = np.linspace(0, limit_std, nlevels)
                norm_std = BoundaryNorm(
                    bound_std,
                    ncolors=cc.cm.CET_D9.N,
                    clip=False
                )





                for i, ((ax_mean, ax_std), (label, agg)) in enumerate(zip(zip(axes_mean, axes_std), depth_results)):

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
                        s=size_from_n(agg["nobs"]),
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

                fig_mean.legend(
                    handles,
                    [f"{v} obs" for v in legend_vals],
                    title="# of obs",
                    loc="center left",
                    bbox_to_anchor=(0.86, 0.5),   # pushes it outside right side
                    frameon=True,
                    borderaxespad=0.0,
                    fontsize=9,
                    title_fontsize=10
                )
                fig_std.legend(
                    handles,
                    [f"{v} obs" for v in legend_vals],
                    title="# of obs",
                    loc="center left",
                    bbox_to_anchor=(0.86, 0.5),
                    frameon=True,
                    borderaxespad=0.0,
                    fontsize=9,
                    title_fontsize=10
                )
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
    
    # for plotting diags associated with specific output
    surface_mapper.download_output_files(['wod_t_pfl.nc',
                                          'wod_t_gld.nc',
                                          'wod_t_drb.nc',
                                          'wod_t_xbt.nc',
                                          'sst_viirs_n20_l3u.nc',
                                          'sst_viirs_npp_l3u.nc',
                                          'sst_avhrr_mc_l3u.nc',
                                          'sst_avhrr_mb_l3u.nc',
                                          'icec_amsr2_north.nc',
                                          'icec_amsr2_south.nc',
                                          #'...'
                                          ])
    surface_mapper.map_soca_obs(soca_obs_dir='soca_diags_mapper')

def main():
    """Main entry point.
    """
    run()

if __name__=='__main__':
    main()