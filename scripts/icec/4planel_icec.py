#!/usr/bin/env python3
"""
SOCA-JEDI + AMSR2 IceC Innovations Dashboard
Generates a single 4-panel figure (2x2 grid) containing:
Top Row: North Pole O-B & O-A | Bottom Row: South Pole O-B & O-A
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
parser = argparse.ArgumentParser(description="SOCA-JEDI + AMSR2 IceC 4-Panel Dashboard")
parser.add_argument(
    "--timestamp", 
    type=str, 
    required=True, 
    help="The run timestamp for the main titles (e.g., 'July 01 - 07 2023')"
)
args = parser.parse_args()
timestamp_parts = args.timestamp.split()
start_day = timestamp_parts[1] if len(timestamp_parts) > 1 else "01"

# =====================================================================
# 2. CONFIGURATION & USER SETTINGS
# =====================================================================
NORTH_FILES_PATH = "*icec*north*.nc"
SOUTH_FILES_PATH = "*icec*south*.nc"
OUTPUT_DIR = "./jedi_icec_diagnostics"

RUN_TIMESTAMP = args.timestamp
os.makedirs(OUTPUT_DIR, exist_ok=True)

VARS = {
    'lon': 'MetaData_longitude',
    'lat': 'MetaData_latitude',
    'icec_obs': 'ObsValue_seaIceFraction',
    'icec_ombg': 'ombg_seaIceFraction',
    'icec_oman': 'oman_seaIceFraction'
}

# =====================================================================
# 3. IODA MULTI-GROUP LOADER FUNCTION
# =====================================================================
def load_hemisphere_data(file_pattern, hemisphere_name):
    print(f"Searching for {hemisphere_name} files...")
    file_list = sorted(glob.glob(file_pattern))
    if not file_list:
        print(f"Warning: No NetCDF files found matching '{file_pattern}'")
        return None

    print(f" -> Merging {len(file_list)} files for {hemisphere_name}...")
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
    ds_clean = ds.dropna(dim='Location', how='any', subset=[VARS['icec_obs']]).compute()
    
    if len(ds_clean['Location']) == 0:
        return None

    for vkey in ['icec_ombg', 'icec_oman']:
        vname = VARS[vkey]
        if ds_clean[vname].notnull().any():
            icec_condition = (abs(ds_clean[vname]) <= 1.0) | ds_clean[vname].isnull()
            ds_clean = ds_clean.where(icec_condition, drop=True)
            
    return ds_clean

ds_north = load_hemisphere_data(NORTH_FILES_PATH, "Northern Hemisphere")
ds_south = load_hemisphere_data(SOUTH_FILES_PATH, "Southern Hemisphere")

# =====================================================================
# 4. PLOT GENERATION: 2x2 DASHBOARD MATRIX
# =====================================================================
if ds_north is not None or ds_south is not None:
    print("Generating 4-panel unified diagnostics dashboard...")
    
    # Create the figure object explicitly matching a square layout
    fig = plt.figure(figsize=(16, 14))
    
    vmin, vmax = -0.5, 0.5
    cmap = 'RdBu_r'
    sc_master = None # Handle to pass coordinates to colorbar safely

    # -----------------------------------------------------------------
    # PANEL 1: NORTH POA - O-B (Top Left)
    # -----------------------------------------------------------------
    ax1 = fig.add_subplot(2, 2, 1, projection=ccrs.NorthPolarStereo(central_longitude=-45))
    ax1.set_extent([-180, 180, 50, 90], ccrs.PlateCarree())
    ax1.add_feature(cfeature.LAND, facecolor='lightgrey', zorder=2)
    ax1.coastlines(linewidth=0.8, zorder=3)
    ax1.gridlines(draw_labels=False, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    ax1.set_title("Northern Hemisphere: Forecast Error (O-B)", fontsize=12, weight='bold')
    
    if ds_north is not None:
        sc_master = ax1.scatter(
            ds_north[VARS['lon']], ds_north[VARS['lat']], c=ds_north[VARS['icec_ombg']], 
            cmap=cmap, vmin=vmin, vmax=vmax, s=1.2, transform=ccrs.PlateCarree(), zorder=1
        )
    else:
        ax1.text(0.5, 0.5, 'No Northern Data Available', transform=ax1.transAxes, ha='center')

    # -----------------------------------------------------------------
    # PANEL 2: NORTH POLE - O-A (Top Right)
    # -----------------------------------------------------------------
    ax2 = fig.add_subplot(2, 2, 2, projection=ccrs.NorthPolarStereo(central_longitude=-45))
    ax2.set_extent([-180, 180, 50, 90], ccrs.PlateCarree())
    ax2.add_feature(cfeature.LAND, facecolor='lightgrey', zorder=2)
    ax2.coastlines(linewidth=0.8, zorder=3)
    ax2.gridlines(draw_labels=False, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    ax2.set_title("Northern Hemisphere: Analysis Residual (O-A)", fontsize=12, weight='bold')
    
    if ds_north is not None:
        ax2.scatter(
            ds_north[VARS['lon']], ds_north[VARS['lat']], c=ds_north[VARS['icec_oman']], 
            cmap=cmap, vmin=vmin, vmax=vmax, s=1.2, transform=ccrs.PlateCarree(), zorder=1
        )

    # -----------------------------------------------------------------
    # PANEL 3: SOUTH POLE - O-B (Bottom Left)
    # -----------------------------------------------------------------
    ax3 = fig.add_subplot(2, 2, 3, projection=ccrs.SouthPolarStereo(central_longitude=0))
    ax3.set_extent([-180, 180, -90, -55], ccrs.PlateCarree())
    ax3.add_feature(cfeature.LAND, facecolor='lightgrey', zorder=2)
    ax3.coastlines(linewidth=0.8, zorder=3)
    ax3.gridlines(draw_labels=False, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    ax3.set_title("Southern Hemisphere: Forecast Error (O-B)", fontsize=12, weight='bold')
    
    if ds_south is not None:
        sc_s = ax3.scatter(
            ds_south[VARS['lon']], ds_south[VARS['lat']], c=ds_south[VARS['icec_ombg']], 
            cmap=cmap, vmin=vmin, vmax=vmax, s=1.2, transform=ccrs.PlateCarree(), zorder=1
        )
        if sc_master is None:
            sc_master = sc_s
    else:
        ax3.text(0.5, 0.5, 'No Southern Data Available', transform=ax3.transAxes, ha='center')

    # -----------------------------------------------------------------
    # PANEL 4: SOUTH POLE - O-A (Bottom Right)
    # -----------------------------------------------------------------
    ax4 = fig.add_subplot(2, 2, 4, projection=ccrs.SouthPolarStereo(central_longitude=0))
    ax4.set_extent([-180, 180, -90, -55], ccrs.PlateCarree())
    ax4.add_feature(cfeature.LAND, facecolor='lightgrey', zorder=2)
    ax4.coastlines(linewidth=0.8, zorder=3)
    ax4.gridlines(draw_labels=False, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    ax4.set_title("Southern Hemisphere: Analysis Residual (O-A)", fontsize=12, weight='bold')
    
    if ds_south is not None:
        ax4.scatter(
            ds_south[VARS['lon']], ds_south[VARS['lat']], c=ds_south[VARS['icec_oman']], 
            cmap=cmap, vmin=vmin, vmax=vmax, s=1.2, transform=ccrs.PlateCarree(), zorder=1
        )

    # -----------------------------------------------------------------
    # UCONSOLIDATED COLORBAR AND TITLES
    # -----------------------------------------------------------------
    # Adjust subplot margins slightly to fit titles and a right colorbar nicely
    fig.subplots_adjust(left=0.05, right=0.88, top=0.88, bottom=0.05, wspace=0.1, hspace=0.15)
    
    # Establish single side colorbar axis [left, bottom, width, height]
    cbar_ax = fig.add_axes([0.91, 0.15, 0.025, 0.65])
    cbar = fig.colorbar(sc_master, cax=cbar_ax, orientation='vertical')
    cbar.set_label('Innovation Intensity (Sea Ice Fraction Gaps)', weight='bold', fontsize=12)

    fig.suptitle(f"AMSR2 Sea Ice Concentration Innovations\nDate: {RUN_TIMESTAMP}", 
                 fontsize=18, weight='bold', y=0.96)
    
    out_img = f"{OUTPUT_DIR}/AMSR2_icec_comprehensive_dashboard_{start_day}.png"
    plt.savefig(out_img, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\nDashboard compiled beautifully! Image saved to: {out_img}")

else:
    print("Error: No data available to generate the consolidated dashboard.")
