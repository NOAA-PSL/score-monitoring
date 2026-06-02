#!/usr/bin/env python3
"""
SOCA-JEDI + AMSR2 IceC Daily Diagnostics: Assimilation Increments
Computes and visualizes (Analysis - Background) to show exactly where 
and how much ice the data assimilation system added or removed today.
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
parser = argparse.ArgumentParser(description="SOCA-JEDI Daily IceC Increment Generator")
parser.add_argument(
    "--timestamp", 
    type=str, 
    required=True, 
    help="The run date/timestamp for the main titles (e.g., '2023-07-01')"
)
args = parser.parse_args()

# Clean up string for filenames
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
    'icec_obs': 'ObsValue_seaIceFraction',
    'icec_ombg': 'ombg_seaIceFraction',
    'icec_oman': 'oman_seaIceFraction'
}

# =====================================================================
# 3. DATA LOADER & INCREMENT CALCULATION FUNCTION
# =====================================================================
def load_and_calculate_increment(file_pattern, hemisphere_name):
    print(f"Searching for daily {hemisphere_name} files...")
    file_list = sorted(glob.glob(file_pattern))
    if not file_list:
        print(f"Warning: No NetCDF files found matching '{file_pattern}' for today.")
        return None

    print(f" -> Processing {len(file_list)} files for {hemisphere_name}...")
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
    
    # Compute the array into memory early to prevent Dask bottlenecks
    ds_clean = ds.dropna(dim='Location', how='any', subset=[VARS['icec_obs']]).compute()
    
    # Clean standard innovations
    for vkey in ['icec_ombg', 'icec_oman']:
        vname = VARS[vkey]
        if ds_clean[vname].notnull().any():
            icec_condition = (abs(ds_clean[vname]) <= 1.0) | ds_clean[vname].isnull()
            ds_clean = ds_clean.where(icec_condition, drop=True)
            
    # CRITICAL CALCULATION: Compute the Analysis Increment (A - B)
    # If JEDI brought the model closer to observations, this isolates that physical shock.
    ds_clean['increment'] = ds_clean[VARS['icec_ombg']] - ds_clean[VARS['icec_oman']]
    
    return ds_clean

# Load and calculate for both poles
ds_north = load_and_calculate_increment(NORTH_FILES_PATH, "Northern Hemisphere")
ds_south = load_and_calculate_increment(SOUTH_FILES_PATH, "Southern Hemisphere")

# =====================================================================
# 4. PLOT GENERATION: DUAL-PANEL DAILY INCREMENTS
# =====================================================================
if ds_north is not None or ds_south is not None:
    print("Generating Dual Hemisphere Daily Increment Map...")
    
    fig = plt.figure(figsize=(16, 8))
    
    # Tightening the limits for daily increments since changes are subtle
    # Red: JEDI added ice to the model | Blue: JEDI stripped ice from the model
    vmin, vmax = -0.2, 0.2  
    cmap = 'bwr' # Blue-White-Red provides a sharp white neutral zone at 0.0
    
    # --- LEFT PANEL: NORTH POLAR ---
    ax_n = fig.add_subplot(1, 2, 1, projection=ccrs.NorthPolarStereo(central_longitude=-45))
    ax_n.set_extent([-180, 180, 50, 90], ccrs.PlateCarree())
    ax_n.add_feature(cfeature.LAND, facecolor='lightgrey', zorder=2)
    ax_n.coastlines(linewidth=0.8, zorder=3)
    gl_n = ax_n.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl_n.top_labels = False
    gl_n.right_labels = False
    
    if ds_north is not None and len(ds_north['Location']) > 0:
        sc_n = ax_n.scatter(
            ds_north[VARS['lon']], ds_north[VARS['lat']], 
            c=ds_north['increment'], 
            cmap=cmap, vmin=vmin, vmax=vmax, s=2.5, 
            transform=ccrs.PlateCarree(), zorder=1
        )
        ax_n.set_title("Northern Hemisphere", fontsize=12, weight='bold')
    else:
        ax_n.text(0.5, 0.5, 'No Northern Data Available Today', transform=ax_n.transAxes, ha='center')

    # --- RIGHT PANEL: SOUTH POLAR ---
    ax_s = fig.add_subplot(1, 2, 2, projection=ccrs.SouthPolarStereo(central_longitude=0))
    ax_s.set_extent([-180, 180, -90, -55], ccrs.PlateCarree())
    ax_s.add_feature(cfeature.LAND, facecolor='lightgrey', zorder=2)
    ax_s.coastlines(linewidth=0.8, zorder=3)
    gl_s = ax_s.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl_s.top_labels = False
    gl_s.right_labels = False
    
    if ds_south is not None and len(ds_south['Location']) > 0:
        sc_s = ax_s.scatter(
            ds_south[VARS['lon']], ds_south[VARS['lat']], 
            c=ds_south['increment'], 
            cmap=cmap, vmin=vmin, vmax=vmax, s=2.5, 
            transform=ccrs.PlateCarree(), zorder=1
        )
        ax_s.set_title("Southern Hemisphere", fontsize=12, weight='bold')
    else:
        ax_s.text(0.5, 0.5, 'No Southern Data Available Today', transform=ax_s.transAxes, ha='center')

    # Establish the master colorbar link
    active_sc = sc_n if (ds_north is not None and len(ds_north['Location']) > 0) else sc_s
    
    fig.subplots_adjust(bottom=0.15, top=0.85, wspace=0.1)
    cbar_ax = fig.add_axes([0.25, 0.08, 0.5, 0.03])
    
    cbar = fig.colorbar(active_sc, cax=cbar_ax, orientation='horizontal')
    cbar.set_label('Assimilation Increment [Analysis - Background] (Ice Fraction)', weight='bold')
    
    fig.suptitle(f"Daily Sea Ice Assimilation Increments (Analysis - Background)\nRun Date: {args.timestamp}", 
                 fontsize=15, weight='bold', y=0.95)
    
    out_img = f"{OUTPUT_DIR}/AMSR2_icec_daily_increment_{file_date}.png"
    plt.savefig(out_img, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\nSuccess! Daily increment map saved to: {out_img}")

else:
    print("Error: No data available to compute daily increments for either pole.")
