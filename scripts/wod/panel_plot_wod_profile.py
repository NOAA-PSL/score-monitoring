import os
import glob
import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset

# ==========================================
# 1. USER CONFIGURATION
# ==========================================
data_dir = "/Users/sfredrick/joao/diag_files/hold/" 

# Define your vertical profile grid (0m down to 2000m, tracking every 25m)
depth_bins = np.arange(0, 2051, 25)
bin_centers = (depth_bins[:-1] + depth_bins[1:]) / 2

# ==========================================
# 2. DYNAMIC DATE PROCESSING
# ==========================================
all_nc_files = glob.glob(os.path.join(data_dir, "wod_*.nc"))
unique_dates = sorted(list(set([os.path.basename(f).split('_')[3] for f in all_nc_files])))
unique_dates = unique_dates[:7]  # Limit to 7 days maximum

print(f"Found {len(unique_dates)} unique days to process: {unique_dates}")

# ==========================================
# 3. TAILORED JEDI PROCESSING FUNCTION
# ==========================================
def calculate_daily_profile(date_str, var_prefix, var_name):
    """Pools the 4 daily synoptic cycles, filters using final EffectiveQC1, 

    and averages the pre-calculated ombg/oman residuals into depth bins.
    """
    file_pattern = os.path.join(data_dir, f"wod_{var_prefix}_pfl_{date_str}_*.nc")
    daily_files = glob.glob(file_pattern)
    
    pool_ombg, pool_oman, pool_depths = [], [], []
    
    for file_path in daily_files:
        with Dataset(file_path, 'r') as nc:
            # Verify the variable exists in this file type
            if var_name not in nc.groups['ObsValue'].variables:
                continue
            
            # Extract variables based on your exact ncdump schema
            ombg = nc.groups['ombg'].variables[var_name][:]
            oman = nc.groups['oman'].variables[var_name][:]
            qc = nc.groups['EffectiveQC1'].variables[var_name][:]
            depth = nc.groups['MetaData'].variables['depth'][:]
            
            # Handle netCDF4 masked arrays safely
            if hasattr(ombg, 'filled'): ombg = ombg.filled(np.nan)
            if hasattr(oman, 'filled'): oman = oman.filled(np.nan)
            if hasattr(depth, 'filled'): depth = depth.filled(np.nan)
            if hasattr(qc, 'filled'): qc = qc.filled(-999)
            
            # Apply JEDI Quality Control Filter: Only keep records where EffectiveQC1 == 0
            qc_mask = (qc == 0) & (~np.isnan(ombg)) & (~np.isnan(oman)) & (~np.isnan(depth))
            
            pool_ombg.extend(ombg[qc_mask])
            pool_oman.extend(oman[qc_mask])
            pool_depths.extend(depth[qc_mask])
            
    if len(pool_ombg) == 0:
        return np.full(len(bin_centers), np.nan), np.full(len(bin_centers), np.nan)
        
    pool_ombg = np.array(pool_ombg)
    pool_oman = np.array(pool_oman)
    pool_depths = np.array(pool_depths)
    
    # Bin the data points vertically to build the profile
    rmse_ombg = np.zeros(len(bin_centers))
    rmse_oman = np.zeros(len(bin_centers))
    
    for i in range(len(depth_bins) - 1):
        idx = (pool_depths >= depth_bins[i]) & (pool_depths < depth_bins[i+1])
        if np.sum(idx) > 0:
            # The groups 'ombg' and 'oman' are already the (Obs - Model) residuals
            rmse_ombg[i] = np.sqrt(np.mean(pool_ombg[idx]**2))
            rmse_oman[i] = np.sqrt(np.mean(pool_oman[idx]**2))
        else:
            rmse_ombg[i] = np.nan
            rmse_oman[i] = np.nan
            
    return rmse_ombg, rmse_oman

# ==========================================
# 4. HARVEST DATA ACROSS THE TIMELINE
# ==========================================
temp_ob_all, temp_oa_all = [], []
salt_ob_all, salt_oa_all = [], []

print("\nExtracting data, pooling diurnal cycles, and applying JEDI QC filters...")
for day_idx, date_str in enumerate(unique_dates):
    print(f" Processing Day {day_idx+1} ({date_str})...")
    
    # Process Temperature (Prefix 't', IODA variable name 'waterTemperature')
    t_ob, t_oa = calculate_daily_profile(date_str, 't', 'waterTemperature')
    temp_ob_all.append(t_ob)
    temp_oa_all.append(t_oa)
    
    # Process Salinity (Prefix 's', IODA variable name 'salinity')
    s_ob, s_oa = calculate_daily_profile(date_str, 's', 'salinity')
    salt_ob_all.append(s_ob)
    salt_oa_all.append(s_oa)

# Convert to structured 2D matrices [Days x Depth Bins]
temp_ob = np.array(temp_ob_all)
temp_oa = np.array(temp_oa_all)
salt_ob = np.array(salt_ob_all)
salt_oa = np.array(salt_oa_all)

# Compute statistical averages and daily bounds safely
mean_temp_ob, min_temp_ob, max_temp_ob = np.nanmean(temp_ob, axis=0), np.nanmin(temp_ob, axis=0), np.nanmax(temp_ob, axis=0)
mean_temp_oa, min_temp_oa, max_temp_oa = np.nanmean(temp_oa, axis=0), np.nanmin(temp_oa, axis=0), np.nanmax(temp_oa, axis=0)

mean_salt_ob, min_salt_ob, max_salt_ob = np.nanmean(salt_ob, axis=0), np.nanmin(salt_ob, axis=0), np.nanmax(salt_ob, axis=0)
mean_salt_oa, min_salt_oa, max_salt_oa = np.nanmean(salt_oa, axis=0), np.nanmin(salt_oa, axis=0), np.nanmax(salt_oa, axis=0)

# ==========================================
# 5. PLOTTING THE HYBRID JEDI DASHBOARD
# ==========================================
fig, axs = plt.subplots(2, 2, figsize=(16, 14), sharey=True, gridspec_kw={'wspace': 0.05, 'hspace': 0.2})
fig.suptitle("Wod PFL to February 05 - 11 2023 Global RMSE Profiles", fontsize=16, fontweight='bold', y=0.95)
day_colors = plt.cm.plasma(np.linspace(0.1, 0.9, len(unique_dates)))

# --- PANEL A: Temperature Mean Summary (Top Left) ---
axs[0, 0].plot(mean_temp_ob, bin_centers, color='crimson', lw=2.5, label='Mean O-B (Forecast)')
axs[0, 0].fill_betweenx(bin_centers, min_temp_ob, max_temp_ob, color='crimson', alpha=0.15)
axs[0, 0].plot(mean_temp_oa, bin_centers, color='royalblue', lw=2.5, label='Mean O-A (Analysis)')
axs[0, 0].fill_betweenx(bin_centers, min_temp_oa, max_temp_oa, color='royalblue', alpha=0.15)
axs[0, 0].set_title("Temperature Global RMSE (Multi-Day Summary)", fontsize=12, fontweight='bold')
axs[0, 0].set_xlabel("RMSE (°C)")
axs[0, 0].set_ylabel("Depth (m)")
axs[0, 0].legend(loc='lower right')
axs[0, 0].grid(True, linestyle='--', alpha=0.5)

# --- PANEL B: Salinity Mean Summary (Top Right) ---
axs[0, 1].plot(mean_salt_ob, bin_centers, color='darkorange', lw=2.5, label='Mean O-B (Forecast)')
axs[0, 1].fill_betweenx(bin_centers, min_salt_ob, max_salt_ob, color='darkorange', alpha=0.15)
axs[0, 1].plot(mean_salt_oa, bin_centers, color='teal', lw=2.5, label='Mean O-A (Analysis)')
axs[0, 1].fill_betweenx(bin_centers, min_salt_oa, max_salt_oa, color='teal', alpha=0.15)
axs[0, 1].set_title("Salinity Global RMSE (Multi-Day Summary)", fontsize=12, fontweight='bold')
axs[0, 1].set_xlabel("RMSE (psu)")
axs[0, 1].legend(loc='lower right')
axs[0, 1].grid(True, linestyle='--', alpha=0.5)

# --- PANEL C: Temperature Daily Lines (Bottom Left) ---
for i, date_str in enumerate(unique_dates):
    axs[1, 0].plot(temp_ob[i], bin_centers, color=day_colors[i], linestyle='--', alpha=0.5)
    axs[1, 0].plot(temp_oa[i], bin_centers, color=day_colors[i], linestyle='-', alpha=0.9, label=f"Day {i+1} ({date_str[-4:]})")
axs[1, 0].set_title("Temperature Daily Profiles (O-B Dashed, O-A Solid)", fontsize=12, fontweight='bold')
axs[1, 0].set_xlabel("RMSE (°C)")
axs[1, 0].set_ylabel("Depth (m)")
axs[1, 0].legend(loc='lower right', ncol=2, title="Timeline")
axs[1, 0].grid(True, linestyle='--', alpha=0.5)

# --- PANEL D: Salinity Daily Lines (Bottom Right) ---
for i, date_str in enumerate(unique_dates):
    axs[1, 1].plot(salt_ob[i], bin_centers, color=day_colors[i], linestyle='--', alpha=0.5)
    axs[1, 1].plot(salt_oa[i], bin_centers, color=day_colors[i], linestyle='-', alpha=0.9, label=f"Day {i+1} ({date_str[-4:]})")
axs[1, 1].set_title("Salinity Daily Profiles (O-B Dashed, O-A Solid)", fontsize=12, fontweight='bold')
axs[1, 1].set_xlabel("RMSE (psu)")
axs[1, 1].legend(loc='lower right', ncol=2, title="Timeline")
axs[1, 1].grid(True, linestyle='--', alpha=0.5)

# Invert Y-axis for oceanographic orientation
axs[0, 0].invert_yaxis()
axs[0, 0].set_ylim(2050, -50) 

output_name = "jedi_soca_final_dashboard.png"
plt.savefig(output_name, dpi=150, bbox_inches='tight')
plt.show()

print(f"\nSuccess! Visual evaluation grid generated: {output_name}")
