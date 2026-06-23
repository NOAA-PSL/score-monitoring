#!/usr/bin/env python3
"""
SOCA-JEDI + WOD Monthly Diagnostics Generator
Dual-Variable Version: Handles separate or combined Temp & Salinity IODA files.
Fully updated with independent xarray numerical sorting to eliminate loops.
Includes a global runtime timestamp on all generated figures.
"""

import os,sys
import glob
from datetime import datetime
import numpy as np
import xarray as xr
import argparse
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# =====================================================================
# 1. COMMAND LINE ARGUMENTS
# =====================================================================
parser = argparse.ArgumentParser(description="SOCA-JEDI + WOD Monthly Diagnostics Generator")
parser.add_argument(
    "--timestamp", 
    type=str, 
    required=True, 
    help="The run timestamp for the main titles (e.g., 'January 01 - 07 2023')"
)
args = parser.parse_args()
timestamp_parts = args.timestamp.split()
start_day = timestamp_parts[1]
# =====================================================================
# 2. CONFIGURATION & USER SETTINGS
# =====================================================================
DAILY_FILES_PATH = "*.nc" 
OUTPUT_DIR = "./jedi_monthly_diagnostics"

# Generate a clean date and time string for the main titles
RUN_TIMESTAMP = args.timestamp
print(RUN_TIMESTAMP)

# Exact variable paths mapping to your JEDI-IODA structure
VARS = {
    'lon': 'MetaData_longitude',
    'lat': 'MetaData_latitude',
    'depth': 'MetaData_depth',
    
    # Temperature
    't_obs': 'ObsValue_waterTemperature',
    't_ombg': 'ombg_waterTemperature',
    't_oman': 'oman_waterTemperature',
    
    # Salinity 
    's_obs': 'ObsValue_salinity',
    's_ombg': 'ombg_salinity',
    's_oman': 'oman_salinity'
}

TARGET_DEPTHS = [5, 100, 500, 1000] 
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =====================================================================
# 2. IODA MULTI-GROUP LOADER
# =====================================================================
print("Searching for daily files...")
file_list = sorted(glob.glob(DAILY_FILES_PATH))
print(f"Found {len(file_list)} files. Merging IODA groups...")

ioda_groups = ['MetaData', 'ObsValue', 'ombg', 'oman']
group_datasets = []

for group in ioda_groups:
    print(f" -> Extracting and stacking group: '{group}'...")
    ds_g = xr.open_mfdataset(
        file_list, 
        group=group, 
        concat_dim='Location',   # Stacking along JEDI's 'Location' dimension
        combine='nested',
        data_vars='minimal',
        coords='minimal',
        compat='override',
        join='override'          # Suppresses alignment/join FutureWarnings
    )
    ds_g = ds_g.rename({v: f"{group}_{v}" for v in ds_g.data_vars})
    group_datasets.append(ds_g)

ds = xr.merge(group_datasets, join='override')
print("Dataset successfully consolidated via Dask.")

# =====================================================================
# 3. SEPARATE TEMPERATURE AND SALINITY DATASTREAMS & QUALITY CONTROL
# =====================================================================
print("Separating datastreams and applying outlier filters...")

# --- Process Temperature ---
ds_temp = ds.dropna(dim='Location', how='any', subset=[VARS['t_obs']]).compute()

# QC Filter: Drop temperature innovations outside +/- 10.0 degrees C
for vkey in ['t_ombg', 't_oman']:
    vname = VARS[vkey]
    if ds_temp[vname].notnull().any():
        t_condition = (abs(ds_temp[vname]) < 10.0) | ds_temp[vname].isnull()
        ds_temp = ds_temp.where(t_condition, drop=True)

# --- Process Salinity ---
has_salinity = VARS['s_obs'] in ds.data_vars
if has_salinity:
    ds_sal = ds.dropna(dim='Location', how='any', subset=[VARS['s_obs']]).compute()
    
    # QC Filter: Drop salinity innovations outside +/- 2.0 psu (Removes gross errors)
    for vkey in ['s_ombg', 's_oman']:
        vname = VARS[vkey]
        if ds_sal[vname].notnull().any():
            s_condition = (abs(ds_sal[vname]) < 2.0) | ds_sal[vname].isnull()
            ds_sal = ds_sal.where(s_condition, drop=True)
else:
    print(" -> Note: No salinity data detected in this file batch.")

# =====================================================================
# 4. PLOT 1: SIDE-BY-SIDE VERTICAL RMSE PROFILES
# =====================================================================
print("Generating Vertical Performance Profiles...")
fig, axes = plt.subplots(1, 2, figsize=(14, 8), sharey=True)

# --- PANEL 1: TEMPERATURE ---
if len(ds_temp['Location']) > 0:
    t_depths = ds_temp[VARS['depth']]
    t_bin_max = float(t_depths.max().values)
    t_bins = np.arange(0, min(t_bin_max, 2000), 20) # Cap at 2000m for plot clarity
    
    t_rmse_b = np.sqrt((ds_temp[VARS['t_ombg']]**2).groupby_bins(t_depths, t_bins).mean())
    t_rmse_a = np.sqrt((ds_temp[VARS['t_oman']]**2).groupby_bins(t_depths, t_bins).mean())
    
    # Extract the name of the coordinate created by group/by_bins
    bin_dim_t = t_rmse_b.dims[0]
    
    # Convert Interval coordinates to float midpoints and sort each dataset independently
    t_rmse_b = t_rmse_b.assign_coords({bin_dim_t: [b.mid for b in t_rmse_b[bin_dim_t].values]}).sortby(bin_dim_t)
    t_rmse_a = t_rmse_a.assign_coords({bin_dim_t: [b.mid for b in t_rmse_a[bin_dim_t].values]}).sortby(bin_dim_t)
    
    axes[0].plot(t_rmse_b.values, t_rmse_b[bin_dim_t].values, label='O-B RMSE (Forecast)', color='crimson', lw=2)
    axes[0].plot(t_rmse_a.values, t_rmse_a[bin_dim_t].values, label='O-A RMSE (Analysis)', color='royalblue', lw=2)
    axes[0].set_title('Temperature Global RMSE', fontsize=13, weight='bold')
    axes[0].set_xlabel('RMSE (°C)', fontsize=11)
    axes[0].set_ylabel('Depth (m)', fontsize=11)
    axes[0].invert_yaxis()
    axes[0].legend(loc='lower right')
else:
    axes[0].text(0.5, 0.5, 'No Temperature Data', transform=axes[0].transAxes, ha='center')

# --- PANEL 2: SALINITY ---
if has_salinity and len(ds_sal['Location']) > 0:
    s_depths = ds_sal[VARS['depth']]
    s_bin_max = float(s_depths.max().values)
    s_bins = np.arange(0, min(s_bin_max, 2000), 20)
    
    s_rmse_b = np.sqrt((ds_sal[VARS['s_ombg']]**2).groupby_bins(s_depths, s_bins).mean())
    s_rmse_a = np.sqrt((ds_sal[VARS['s_oman']]**2).groupby_bins(s_depths, s_bins).mean())
    
    # Extract the name of the coordinate created by groupby_bins
    bin_dim_s = s_rmse_b.dims[0]
    
    # Convert Interval coordinates to float midpoints and sort each dataset independently
    s_rmse_b = s_rmse_b.assign_coords({bin_dim_s: [b.mid for b in s_rmse_b[bin_dim_s].values]}).sortby(bin_dim_s)
    s_rmse_a = s_rmse_a.assign_coords({bin_dim_s: [b.mid for b in s_rmse_a[bin_dim_s].values]}).sortby(bin_dim_s)
    
    axes[1].plot(s_rmse_b.values, s_rmse_b[bin_dim_s].values, label='O-B RMSE (Forecast)', color='darkorange', lw=2)
    axes[1].plot(s_rmse_a.values, s_rmse_a[bin_dim_s].values, label='O-A RMSE (Analysis)', color='teal', lw=2)
    axes[1].set_title('Salinity Global RMSE', fontsize=13, weight='bold')
    axes[1].set_xlabel('RMSE (psu)', fontsize=13)
    axes[1].legend(loc='lower right')
else:
    axes[1].text(0.5, 0.5, 'No Salinity Data', transform=axes[1].transAxes, ha='center')

# Add main title above both subplots with the execution timestamp
plt.tight_layout()
fig.subplots_adjust(top=0.82)  
fig.suptitle(f" WOD PFL \nDates: {RUN_TIMESTAMP}", 
             fontsize=16, weight='bold', y=0.96)
print(start_day)
exit
plt.savefig(f"{OUTPUT_DIR}/WOD_pfl_202301{start_day}_rmse_profiles.png", dpi=100, bbox_inches='tight')
plt.close()

# =====================================================================
# 5. PLOT 2: SPATIAL INNOVATION MAPS (Both Variables)
# =====================================================================
print("Generating Spatial Innovation Maps...")
for depth_target in TARGET_DEPTHS:
    
    # --- Map Temperature Layers ---
    z_min = max(0, depth_target - 15)
    z_max = depth_target + 15
    depth_range_str = f"{z_min}-{z_max}m"
    t_mask = (ds_temp[VARS['depth']] >= depth_target-15) & (ds_temp[VARS['depth']] <= depth_target+15)
    t_layer = ds_temp.where(t_mask, drop=True)
    
    if len(t_layer['Location']) > 0:
        fig = plt.figure(figsize=(12, 6))
        ax = plt.axes(projection=ccrs.Robinson())
        ax.add_feature(cfeature.LAND, facecolor='lightgrey', zorder=2)
        ax.coastlines(linewidth=0.8, zorder=3)
        
# Add Meridians and Parallels
        gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.6, linestyle='--')
        gl.top_labels = False
        gl.right_labels = False
        gl.xlabel_style = {'size': 9, 'color': 'dimgray'}
        gl.ylabel_style = {'size': 9, 'color': 'dimgray'}


        sc = ax.scatter(t_layer[VARS['lon']], t_layer[VARS['lat']], c=t_layer[VARS['t_ombg']], 
                        cmap='RdBu_r', vmin=-2.0, vmax=2.0, s=8, transform=ccrs.PlateCarree(), zorder=1)
        
        plt.colorbar(sc, ax=ax, orientation='horizontal', pad=0.05, shrink=0.6, label='O-B Innovation (°C)')
        plt.title(f'Temperature Innovations (O-B) {depth_range_str}', fontsize=12, weight='bold')
        
        fig.suptitle(f"WOD PFL: {RUN_TIMESTAMP}", fontsize=10, color='black', y=0.98)
        
        plt.savefig(f"{OUTPUT_DIR}/WOD_pfl_20230205-20230211_temp_{depth_target}m.png", dpi=150, bbox_inches='tight')
        plt.close()

    # --- Map Salinity Layers ---
    if has_salinity:
        z_min = max(0, depth_target - 15)
        z_max = depth_target + 15
        depth_range_str = f"{z_min}-{z_max}m"

        s_mask = (ds_sal[VARS['depth']] >= depth_target-15) & (ds_sal[VARS['depth']] <= depth_target+15)
        s_layer = ds_sal.where(s_mask, drop=True)
        
        if len(s_layer['Location']) > 0:
            fig = plt.figure(figsize=(12, 6))
            ax = plt.axes(projection=ccrs.Robinson())
            ax.add_feature(cfeature.LAND, facecolor='lightgrey', zorder=2)
            ax.coastlines(linewidth=0.8, zorder=3)

            # Gridlines
            gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.6, linestyle='--')
            gl.top_labels = False
            gl.right_labels = False
            gl.xlabel_style = {'size': 9, 'color': 'dimgray'}
            gl.ylabel_style = {'size': 9, 'color': 'dimgray'}
            
            sc = ax.scatter(s_layer[VARS['lon']], s_layer[VARS['lat']], c=s_layer[VARS['s_ombg']], 
                            cmap='BrBG', vmin=-0.5, vmax=0.5, s=8, transform=ccrs.PlateCarree(), zorder=1)
            
            plt.colorbar(sc, ax=ax, orientation='horizontal', pad=0.05, shrink=0.6, label='O-B Innovation (psu)')
            plt.title(f'Salinity Innovations (O-B) {depth_range_str}', fontsize=12, weight='bold')
            
            fig.suptitle(f"WOD PFL: {RUN_TIMESTAMP}", fontsize=10, color='black', y=0.98)
            
            plt.savefig(f"{OUTPUT_DIR}/WOD_pfl_20230205-20230211_sal_{depth_target}m.png", dpi=150, bbox_inches='tight')
            plt.close()

print(f"\nExecution Complete! All files processed.")
print(f"Artifacts successfully generated and saved to: {OUTPUT_DIR}/")
