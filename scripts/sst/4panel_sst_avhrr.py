#!/usr/bin/env python3
"""
SOCA-JEDI + AVHRR SST Global Innovations Dashboard
Generates a single 2-panel figure (2x1 vertical stack) containing:
Top Row: Global O-B | Bottom Row: Global O-A
"""

import os
import sys
import glob
import numpy as np
import xarray as xr
import argparse
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# =====================================================================
# 1. COMMAND LINE ARGUMENTS
# =====================================================================
parser = argparse.ArgumentParser(description="SOCA-JEDI + AVHRR SST Global 2-Panel Dashboard")
parser.add_argument(
    "--timestamp", 
    type=str, 
    required=True, 
    help="The run timestamp for the main titles (e.g., 'July 01 2023')"
)
args = parser.parse_args()
timestamp_parts = args.timestamp.split()
start_day = timestamp_parts[1] if len(timestamp_parts) > 1 else "01"

# =====================================================================
# 2. CONFIGURATION & USER SETTINGS
# =====================================================================
FILE_PATTERNS = ["*sst_avhrr_mb_l3u_*.nc", "*sst_avhrr_mc_l3u_*.nc"]
OUTPUT_DIR = "./jedi_sst_diagnostics"

RUN_TIMESTAMP = args.timestamp
os.makedirs(OUTPUT_DIR, exist_ok=True)

VARS = {
    'lon': 'MetaData_longitude',
    'lat': 'MetaData_latitude',
    'sst_obs': 'ObsValue_seaSurfaceTemperature',
    'sst_ombg': 'ombg_seaSurfaceTemperature',
    'sst_oman': 'oman_seaSurfaceTemperature'
}

# =====================================================================
# 3. IODA MULTI-GROUP LOADER FUNCTION
# =====================================================================
def load_global_data(patterns):
    """Finds all mb and mc files, merges them, and cleans the global data"""
    file_list = []
    for pattern in patterns:
        file_list.extend(glob.glob(pattern))
    
    file_list = sorted(list(set(file_list))) # Remove duplicates and sort
    
    if not file_list:
        print(f"Warning: No NetCDF files found matching patterns: {patterns}")
        return None

    print(f" -> Merging {len(file_list)} AVHRR SST files globally...")
    ioda_groups = ['MetaData', 'ObsValue', 'ombg', 'oman']
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
    ds_clean = ds.dropna(dim='Location', how='any', subset=[VARS['sst_obs']]).compute()
    
    if len(ds_clean['Location']) == 0:
        return None

    # Filter out extreme outlier innovations
    for vkey in ['sst_ombg', 'sst_oman']:
        vname = VARS[vkey]
        if ds_clean[vname].notnull().any():
            sst_condition = (abs(ds_clean[vname]) <= 5.0) | ds_clean[vname].isnull()
            ds_clean = ds_clean.where(sst_condition, drop=True)
            
    return ds_clean

ds_global = load_global_data(FILE_PATTERNS)

# =====================================================================
# 4. PLOT GENERATION: GLOBAL 2-PANEL STACK
# =====================================================================
if ds_global is not None:
    print("Generating global 2-panel unified diagnostics dashboard...")
    
    # Setup standard landscape-oriented figure for rectangular maps
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12), 
                                   subplot_kw={'projection': ccrs.PlateCarree()})
    
    vmin, vmax = -2.0, 2.0
    cmap = 'RdBu_r'

    # -----------------------------------------------------------------
    # PANEL 1: GLOBAL O-B (Top Panel)
    # -----------------------------------------------------------------
    ax1.set_global()
    ax1.add_feature(cfeature.LAND, facecolor='lightgrey', zorder=2)
    ax1.coastlines(linewidth=0.6, zorder=3)
    ax1.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.3, linestyle='--')
    ax1.set_title("Global: Forecast Error (O-B)", fontsize=13, weight='bold')
    
    sc_master = ax1.scatter(
        ds_global[VARS['lon']], ds_global[VARS['lat']], c=ds_global[VARS['sst_ombg']], 
        cmap=cmap, vmin=vmin, vmax=vmax, s=0.4, transform=ccrs.PlateCarree(), zorder=1
    )

    # -----------------------------------------------------------------
    # PANEL 2: GLOBAL O-A (Bottom Panel)
    # -----------------------------------------------------------------
    ax2.set_global()
    ax2.add_feature(cfeature.LAND, facecolor='lightgrey', zorder=2)
    ax2.coastlines(linewidth=0.6, zorder=3)
    ax2.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.3, linestyle='--')
    ax2.set_title("Global: Analysis Residual (O-A)", fontsize=13, weight='bold')
    
    ax2.scatter(
        ds_global[VARS['lon']], ds_global[VARS['lat']], c=ds_global[VARS['sst_oman']], 
        cmap=cmap, vmin=vmin, vmax=vmax, s=0.4, transform=ccrs.PlateCarree(), zorder=1
    )

    # -----------------------------------------------------------------
    # CONSOLIDATED COLORBAR AND TITLES
    # -----------------------------------------------------------------
    # Space panels cleanly to allow longitude/latitude labels to show up properly
    fig.subplots_adjust(left=0.06, right=0.88, top=0.88, bottom=0.06, hspace=0.25)
    
    # Establish a clean vertical side colorbar
    cbar_ax = fig.add_axes([0.91, 0.15, 0.02, 0.65])
    cbar = fig.colorbar(sc_master, cax=cbar_ax, orientation='vertical')
    cbar.set_label('Innovation Intensity ($\Delta$ SST / K)', weight='bold', fontsize=12)

    fig.suptitle(f"AVHRR Global Sea Surface Temperature Innovations\nDate: {RUN_TIMESTAMP}", 
                 fontsize=18, weight='bold', y=0.96)
    
    out_img = f"{OUTPUT_DIR}/AVHRR_sst_global_dashboard_{start_day}.png"
    plt.savefig(out_img, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\nDashboard compiled beautifully! Image saved to: {out_img}")

else:
    print("Error: No data available to generate the consolidated global dashboard.")
