#!/usr/bin/env python3
"""
SOCA-JEDI + AVHRR SST Innovation Histogram Dashboard
Generates a 1x2 subplot dashboard displaying:
Left Panel: Forecast Error (O-B) Distribution
Right Panel: Analysis Residual (O-A) Distribution
"""

import os
import sys
import glob
import numpy as np
import xarray as xr
import argparse
import matplotlib.pyplot as plt

# =====================================================================
# 1. COMMAND LINE ARGUMENTS
# =====================================================================
parser = argparse.ArgumentParser(description="SOCA-JEDI + AVHRR SST Innovation Histograms")
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
    ioda_groups = ['ObsValue', 'ombg', 'oman']
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

    # Filter out extreme outlier innovations to preserve histogram resolution
    for vkey in ['sst_ombg', 'sst_oman']:
        vname = VARS[vkey]
        if ds_clean[vname].notnull().any():
            sst_condition = (abs(ds_clean[vname]) <= 5.0) | ds_clean[vname].isnull()
            ds_clean = ds_clean.where(sst_condition, drop=True)
            
    return ds_clean

ds_global = load_global_data(FILE_PATTERNS)

# =====================================================================
# 4. PLOT GENERATION: 1x2 HISTOGRAM PANELS
# =====================================================================
if ds_global is not None:
    print("Extracting innovation vectors and compiling metrics...")
    
    # Extract data arrays and strip out any remaining NaNs for calculation safety
    ombg = ds_global[VARS['sst_ombg']].dropna(dim='Location').values
    oman = ds_global[VARS['sst_oman']].dropna(dim='Location').values
    
    # Compute statistical descriptors
    n_obs = len(ombg)
    
    mean_ombg = np.mean(ombg)
    std_ombg  = np.std(ombg)
    rmsd_ombg = np.sqrt(np.mean(ombg**2))
    
    mean_oman = np.mean(oman)
    std_oman  = np.std(oman)
    rmsd_oman = np.sqrt(np.mean(oman**2))

    print("Generating histogram dashboard layout...")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), sharex=True)
    
    # Define consistent binning strategy across both plots
    bin_edges = np.linspace(-3.0, 3.0, 120)
    
    # -----------------------------------------------------------------
    # PANEL 1: FORECAST ERROR (O-B) HISTOGRAM
    # -----------------------------------------------------------------
    ax1.hist(ombg, bins=bin_edges, color='steelblue', edgecolor='black', alpha=0.85,
             label=f"N = {n_obs:,}\nMean = {mean_ombg:.3f} K\n$\sigma$ = {std_ombg:.3f} K\nRMSD = {rmsd_ombg:.3f} K")
    
    ax1.axvline(0, color='red', linestyle='--', linewidth=1.2, alpha=0.7)
    ax1.axvline(mean_ombg, color='darkorange', linestyle='-', linewidth=1.5, label=f"Bias Reference")
    ax1.set_title("Forecast Error ($O-B$) Distribution", fontsize=13, weight='bold')
    ax1.set_xlabel("Innovation ($\Delta$ SST / K)", fontsize=11)
    ax1.set_ylabel("Frequency (Observation Count)", fontsize=11)
    ax1.grid(True, linestyle=':', alpha=0.6)
    ax1.legend(loc='upper right', frameon=True, facecolor='white', edgecolor='gainsboro', fontsize=10)

    # -----------------------------------------------------------------
    # PANEL 2: ANALYSIS RESIDUAL (O-A) HISTOGRAM
    # -----------------------------------------------------------------
    ax2.hist(oman, bins=bin_edges, color='seagreen', edgecolor='black', alpha=0.85,
             label=f"N = {n_obs:,}\nMean = {mean_oman:.3f} K\n$\sigma$ = {std_oman:.3f} K\nRMSD = {rmsd_oman:.3f} K")
    
    ax2.axvline(0, color='red', linestyle='--', linewidth=1.2, alpha=0.7)
    ax2.axvline(mean_oman, color='darkorange', linestyle='-', linewidth=1.5)
    ax2.set_title("Analysis Residual ($O-A$) Distribution", fontsize=13, weight='bold')
    ax2.set_xlabel("Innovation ($\Delta$ SST / K)", fontsize=11)
    ax2.grid(True, linestyle=':', alpha=0.6)
    ax2.legend(loc='upper right', frameon=True, facecolor='white', edgecolor='gainsboro', fontsize=10)

    # -----------------------------------------------------------------
    # SUPERTITLE AND EXPORT
    # -----------------------------------------------------------------
    fig.suptitle(f"AVHRR Sea Surface Temperature Innovation Metrics\nDate: {RUN_TIMESTAMP}", 
                 fontsize=16, weight='bold', y=0.98)
    
    fig.tight_layout()
    
    out_img = f"{OUTPUT_DIR}/AVHRR_sst_innovation_histograms_{start_day}.png"
    plt.savefig(out_img, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\nHistogram dashboard successfully compiled! Image saved to: {out_img}")

else:
    print("Error: No data available to generate the innovation histogram plot.")
