#!/usr/bin/env python3
"""
SOCA-JEDI + AMSR2 IceC Daily Diagnostics: Track Mapping
Plots satellite observation times (Hour of Day UTC) to map out the 
physical orbital swaths and ensure full 24-hour data coverage.
"""

import os
import sys
import glob
import numpy as np
import xarray as xr
import pandas as pd
import argparse
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# =====================================================================
# 1. COMMAND LINE ARGUMENTS
# =====================================================================
parser = argparse.ArgumentParser(description="SOCA-JEDI Daily IceC Track Map Generator")
parser.add_argument(
    "--timestamp", 
    type=str, 
    required=True, 
    help="The run date/timestamp for the main titles (e.g., '2023-07-01')"
)
args = parser.parse_args()
file_date = args.timestamp.replace("-", "").replace(" ", "_")

# =====================================================================
# 2. CONFIGURATION & USER SETTINGS
# =====================================================================
NORTH_FILES_PATH = "*icec*north*.nc"
SOUTH_FILES_PATH = "*icec*south*.nc"
OUTPUT_DIR = "./jedi_icec_daily_diagnostics"

os.makedirs(OUTPUT_DIR, exist_ok=True)

VARS = {
    'lon': 'MetaData_longitude',
    'lat': 'MetaData_latitude',
    'time': 'MetaData_dateTime',   
    'icec_obs': 'ObsValue_seaIceFraction'
}

# =====================================================================
# 3. DATA LOADER & TIME CONVERSION FUNCTION (FIXED)
# =====================================================================
def load_and_convert_time(file_pattern, hemisphere_name):
    print(f"Searching for daily {hemisphere_name} files...")
    file_list = sorted(glob.glob(file_pattern))
    if not file_list:
        print(f"Warning: No NetCDF files found matching '{file_pattern}' for today.")
        return None

    print(f" -> Processing {len(file_list)} files for {hemisphere_name}...")
    ioda_groups = ['MetaData', 'ObsValue']
    group_datasets = []

    for group in ioda_groups:
        ds_g = xr.open_mfdataset(
            file_list, 
            group=group, 
            concat_dim='Location', 
            combine='nested',
            data_vars='minimal',
            coords='minimal',
            compat='override',
            join='override' 
        )
        ds_g = ds_g.rename({v: f"{group}_{v}" for v in ds_g.data_vars})
        group_datasets.append(ds_g)

    ds = xr.merge(group_datasets, join='override')
    ds_clean = ds.dropna(dim='Location', how='any', subset=[VARS['icec_obs']]).compute()
    
    if len(ds_clean['Location']) == 0:
        return None

    # SAFELY DETECT AND CONVERT TIMESTAMPS
    raw_time = ds_clean[VARS['time']]
    
    # Check if xarray already decoded it automatically to datetime64
    if np.issubdtype(raw_time.dtype, np.datetime64):
        print(f" -> Time already decoded natively as datetime64.")
        dt_index = pd.DatetimeIndex(raw_time.values)
        hours = dt_index.hour + (dt_index.minute / 60.0) + (dt_index.second / 3600.0)
    else:
        print(f" -> Time found as raw integers. Computing fractional hours via epoch math.")
        # Total seconds modulo seconds in a day (86400) gives current seconds past midnight UTC
        total_seconds = raw_time.values
        seconds_in_day = total_seconds % 86400
        hours = seconds_in_day / 3600.0

    ds_clean['hour_of_day'] = ('Location', hours)
    
    return ds_clean

# Load data and process coordinates
ds_north = load_and_convert_time(NORTH_FILES_PATH, "Northern Hemisphere")
ds_south = load_and_convert_time(SOUTH_FILES_PATH, "Southern Hemisphere")

# =====================================================================
# 4. PLOT GENERATION: DUAL-PANEL DAILY ORBITAL SWATHS
# =====================================================================
if ds_north is not None or ds_south is not None:
    print("Generating Dual Hemisphere Daily Track Map...")
    
    fig = plt.figure(figsize=(16, 8))
    
    # 0 to 24 Hours UTC
    vmin, vmax = 0.0, 24.0  
    cmap = 'twilight' 
    
    # --- LEFT PANEL: NORTH POLAR ---
    ax_n = fig.add_subplot(1, 2, 1, projection=ccrs.NorthPolarStereo(central_longitude=-45))
    ax_n.set_extent([-180, 180, 50, 90], ccrs.PlateCarree())
    ax_n.add_feature(cfeature.LAND, facecolor='lightgrey', zorder=2)
    ax_n.coastlines(linewidth=0.8, zorder=3)
    gl_n = ax_n.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl_n.top_labels = False
    gl_n.right_labels = False
    
    if ds_north is not None:
        sc_n = ax_n.scatter(
            ds_north[VARS['lon']], ds_north[VARS['lat']], 
            c=ds_north['hour_of_day'], 
            cmap=cmap, vmin=vmin, vmax=vmax, s=1.5, 
            transform=ccrs.PlateCarree(), zorder=1
        )
        ax_n.set_title("Northern Hemisphere Swaths", fontsize=12, weight='bold')
    else:
        ax_n.text(0.5, 0.5, 'No Northern Track Data Available', transform=ax_n.transAxes, ha='center')

    # --- RIGHT PANEL: SOUTH POLAR ---
    ax_s = fig.add_subplot(1, 2, 2, projection=ccrs.SouthPolarStereo(central_longitude=0))
    ax_s.set_extent([-180, 180, -90, -55], ccrs.PlateCarree())
    ax_s.add_feature(cfeature.LAND, facecolor='lightgrey', zorder=2)
    ax_s.coastlines(linewidth=0.8, zorder=3)
    gl_s = ax_s.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl_s.top_labels = False
    gl_s.right_labels = False
    
    if ds_south is not None:
        sc_s = ax_s.scatter(
            ds_south[VARS['lon']], ds_south[VARS['lat']], 
            c=ds_south['hour_of_day'], 
            cmap=cmap, vmin=vmin, vmax=vmax, s=1.5, 
            transform=ccrs.PlateCarree(), zorder=1
        )
        ax_s.set_title("Southern Hemisphere Swaths", fontsize=12, weight='bold')
    else:
        ax_s.text(0.5, 0.5, 'No Southern Track Data Available', transform=ax_s.transAxes, ha='center')

    # Establish master colorbar link
    active_sc = sc_n if ds_north is not None else sc_s
    
    fig.subplots_adjust(bottom=0.15, top=0.85, wspace=0.1)
    cbar_ax = fig.add_axes([0.25, 0.08, 0.5, 0.03])
    
    cbar = fig.colorbar(active_sc, cax=cbar_ax, orientation='horizontal')
    cbar.set_label('Observation Time (Hour of Day UTC)', weight='bold')
    cbar.set_ticks([0, 4, 8, 12, 16, 20, 24])
    
    fig.suptitle(f"Daily Satellite Data Coverage Tracks (AMSR2 Swaths)\nRun Date: {args.timestamp}", 
                 fontsize=15, weight='bold', y=0.95)
    
    out_img = f"{OUTPUT_DIR}/AMSR2_icec_daily_tracks_{file_date}.png"
    plt.savefig(out_img, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\nSuccess! Daily track coverage map saved to: {out_img}")

else:
    print("Error: No data available to build a track map for either pole.")
