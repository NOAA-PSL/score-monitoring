#!/usr/bin/env python

import os
import sys
from pathlib import Path
from collections import namedtuple
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime as dt

import xarray as xr
import numpy as np
import cftime
from netCDF4 import MFDataset

from score_hv.config_base import ConfigInterface
from score_hv import stats_utils

HARVESTER_NAME = 'daily_bfg'
VALID_STATISTICS = ('mean', 'variance', 'minimum', 'maximum')

"""Variables of interest that come from the background forecast data.
Commented out variables can be uncommented to generate gridcell weighted
statistics but are in development and are currently not fully supported.
"""
VALID_VARIABLES  = (#'icetk', # sea ice thickness (m)
                    #'lhtfl_ave', # surface latent heat flux (W m^-2)
                    'netrf_avetoa', # top of atmosphere net radiation flux (SW and LW) (W m^-2)
                    #'prate_ave', # surface precip rate (mm weq. s^-1)
                    'prateb_ave', # bucket surface precip rate (mm weq. s^-1)
                    #'pressfc', # surface pressure (Pa)
                    #'snod', # surface snow depth (m)
                    #'soil4', # liquid soil moisture at layer-4 (?)
                    #'soilm', # total column soil moisture content (mm weq.)
                    #'soilt4', # soil temperature unknown layer 4 (K)
                    #'tg3', # deep soil temperature (K)
                    'tmp2m', # 2m (surface air) temperature (K)
                    #'tmpsfc', # surface temperature (K)
                    'ulwrf_avetoa', # top of atmos upward longwave flux (W m^-2)
                    #'weasd', # surface snow water equivalent (mm weq.)
                    )

HarvestedData = namedtuple('HarvestedData', ['filenames',
                                             'statistic',
                                             'variable',
                                             'value',
                                             'units',
                                             'mediantime',
                                             'longname'])

def get_gridcell_area_data_path():
    return os.path.join(Path(__file__).parent.parent.resolve(),
                        'data', 'gridcell-area' + 
                        '_noaa-ufs-gefsv13replay-pds' + 
                        '_bfg_control_1536x768_20231116.nc')
@dataclass
class DailyBFGConfig(ConfigInterface):

    config_data: dict = field(default_factory=dict)

    def __post_init__(self):
        self.set_config()

    def set_config(self):
        """ function to set configuration variables from given dictionary
        """
        self.harvest_filenames = self.config_data.get('filenames')
        self.set_stats()
        self.set_variables()

    def set_variables(self):
        """
        set the variables specified by the config dict
        """
        
        self.variables = self.config_data.get('variable')
        for var in self.variables:
            if var not in VALID_VARIABLES:
                msg = ("'%s' is not a supported "
                       "variable to harvest from the background forecast data. "
                       "Please reconfigure the input dictionary using only the "
                       "following variables: %r" % (var, VALID_VARIABLES))
                raise KeyError(msg)
    
    def get_stats(self):
        ''' return list of all stat types based on harvest_config '''
        return self.stats

    def set_stats(self):
        """
        set the statistics specified by the config dict
        """
        self.stats = self.config_data.get('statistic')
        for stat in self.stats:
            if stat not in VALID_STATISTICS:
                msg = ("'%s' is not a supported statistic to harvest from "
                       "daily mean background forecast data. "
                       "Please reconfigure the input dictionary using only the "
                       "following statistics: %r" % (stat, VALID_STATISTICS))
                raise KeyError(msg)

    def get_variables(self):
        ''' return list of all variable types based on harvest_config'''
        return self.variables

@dataclass
class DailyBFGHv(object):
    """ Harvester dataclass used to parse daily mean statistics from 
        background forecast data
    
        Parameters:
        -----------
        config: DailyBFGConfig object containing information used to determine
                which variable to parse the log file
        Methods:
        --------
        get_data: parse descriptive statistics from log files based on input
                  config file
    
                  returns a list of tuples containing specific data
    """
    config: DailyBFGConfig = field(default_factory=DailyBFGConfig)
     
    def get_data(self):
        """ Harvests requested statistics and variables from background 
            forecast data and returns harvested_data, a list of HarvestData 
            tuples
        
            The below routine extracts timestamps from the input data (forecast 
            files), which are expected to represent the temporal endpoints of 
            the (e.g., 3 hour) forecast window. To calculate the temporal 
            midpoint representative of the whole list of input forecast files,
            the routine estimates the time step using a finite difference
            calculation of the timestamps. Shifting the timestamps by minus
            half of the time step gives the temporal midpoints, which are
            then used to determine the multi-file temporal midpoint.
        """
        harvested_data = list()
        
        xr_dataset = xr.open_mfdataset(self.config.harvest_filenames, 
                                       combine='nested', 
                                       concat_dim='time',
                                       decode_times=True)
        gridcell_area_data = xr.open_dataset(get_gridcell_area_data_path())
        gridcell_area_weights = gridcell_area_data.variables['area']

        temporal_endpoints = np.array([cftime.date2num(time,
            'hours since 1951-01-01 00:00:00') for time in xr_dataset['time']])
        
        if len(self.config.harvest_filenames) > 1:
            """ can estimate the time step only if there're more than 1 
                timestamps
            """
            temporal_midpoints = temporal_endpoints - np.gradient(temporal_endpoints)/2.
        
            median_cftime = cftime.num2date(np.median(temporal_midpoints),
                                            'hours since 1951-01-01 00:00:00')
        else:
            median_cftime = cftime.num2date(np.median(temporal_endpoints),
                                            'hours since 1951-01-01 00:00:00')

        for i, variable in enumerate(self.config.get_variables()):
            """ The first nested loop iterates through each requested variable.
            """
            if variable == 'netrf_avetoa': 
                """The variable name netrf_avetoa referes to the top of the 
                atmosphere (TOA) net radiative energy flux. Here, functions 
                from the stats_utils.py file are called to calculate statistics 
                for the required component variables. The TOA net energy flux
                is defined as the following:
                
                netrf_avetoa = dswrf_avetoa - uswrf_avetoa - ulwrf_avetoa,
                
                where dswrf_avetoa, uswrf_avetoa, ulwrf_avetoa are the
                corresponding mean TOA downward shortwave, upward shortwave and
                upward longwave radiation fluxes, respectively.
                """
                longname = "Top of atmosphere net radiative energy flux"
                units = "W/m**2"
                
                try:
                    temporal_means = (
                        np.ma.masked_invalid(xr_dataset['dswrf_avetoa'].mean(
                                                                dim='time',
                                                                skipna=True)) - 
                        np.ma.masked_invalid(xr_dataset['uswrf_avetoa'].mean(
                                                                dim='time',
                                                                skipna=True)) - 
                        np.ma.masked_invalid(xr_dataset['ulwrf_avetoa'].mean(
                                                                dim='time',
                                                                skipna=True)))
                except KeyError as err:
                    msg = (f'{xr_dataset.data_vars} '
                           'do not include all '
                           'variables needed to calculate TOA net '
                           'radiative energy flux '
                           '("dswrf_avetoa," "uswrf_avetoa" and '
                           '"ulwrf_avetoa")')
                    raise KeyError(msg) from err
                                                                
            else:
                variable_data = xr_dataset[variable]
                if 'long_name' in variable_data.attrs:
                    longname = variable_data.attrs['long_name']
                else:
                    longname = "None"
                if 'units' in variable_data.attrs:
                    units = variable_data.attrs['units']
                else:
                    units = "None"
                
                temporal_means = np.ma.masked_invalid(
                    variable_data.mean(dim='time',skipna=True))
            
            expected_value = stats_utils.area_weighted_mean(
                                               temporal_means,
                                               gridcell_area_weights)
            
            for j, statistic in enumerate(self.config.get_stats()):
                """ The second nested loop iterates through each requested 
                    statistic
                """
                if statistic == 'mean':
                    value = expected_value
                elif statistic == 'variance':
                    value = stats_utils.area_weighted_variance(
                                                temporal_means,
                                                gridcell_area_weights,        
                                                expected_value=expected_value)
                elif statistic == 'maximum':
                    value = np.ma.max(temporal_means)
                
                elif statistic == 'minimum':
                    value = np.ma.min(temporal_means)
                
                harvested_data.append(HarvestedData(
                                    self.config.harvest_filenames,
                                    statistic, 
                                    variable,
                                    np.float32(value),
                                    units,
                                    dt.fromisoformat(median_cftime.isoformat()),
                                    longname))
        
        gridcell_area_data.close()
        xr_dataset.close()
        return harvested_data
