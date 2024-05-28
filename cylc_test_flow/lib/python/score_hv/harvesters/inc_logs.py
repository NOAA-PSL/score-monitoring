#!/usr/bin/env python

""" Collection of methods to faciliate retrieval of increment
    descriptive statistics from logs
"""

import os

from collections import namedtuple
from dataclasses import dataclass
from dataclasses import field

from score_hv.config_base import ConfigInterface

HARVESTER_NAME = 'inc_logs'
VALID_STATISTICS = ('mean', 'RMS')
VALID_VARIABLES = ['pt_inc', 's_inc', 'u_inc', 'v_inc', 'SSH', 'Salinity',
                   'Temperature', 'Speed of Currents', 'o3mr_inc', 'sphum_inc',
                   'T_inc', 'delp_inc', 'delz_inc']

HarvestedData = namedtuple('HarvestedData', ['logfile',
                                             'cycletime',
                                             'statistic',
                                             'variable',
                                             'value', 
                                             'units'])
N_TUPLE_ENTRIES = 6                                             

@dataclass
class LogIncCfg(ConfigInterface):
    """ Dataclass to hold and provide configuration information pertaining to
        how the harvester should read increment log files
    
        Parameters:
        -----------
        config_data: dict - contains configuration data parsed from either an
                            input yaml file or input dict
        
    """
    config_data: dict = field(default_factory=dict)
    
    def __post_init__(self):
        self.set_config()
    
    def set_config(self):
        """ function to set configuration variables from given dictionary
        """ 
        self.harvest_filename = self.config_data.get('filename')
        self.cycletime = self.config_data.get('cycletime')
        self.set_stats()
        self.set_variables()
    
    def set_variables(self):
        """
        set the variables specified by the config dict
        """
        
        self.variables = self.config_data.get('variable')
        for var in self.variables:
            if var not in VALID_VARIABLES:
                msg = ("'%s' is not a supported variable to harvest from the incrememt log files. "
                       "Please reconfigure the input dictionary using only the "
                       "following variables: %r" % (var, VALID_VARIABLES))
                raise KeyError(msg)

    def set_stats(self):
        """
        set the statistics specified by the config dict
        """
        self.stats = self.config_data.get('statistic')
        for stat in self.stats:
            if stat not in VALID_STATISTICS:
                msg = ("'%s' is not a supported statistic to harvest from the incrememt log files. "
                       "Please reconfigure the input dictionary using only the "
                       "following statistics: %r" % (stat, VALID_STATISTICS))
                raise KeyError(msg)
    
    def get_stats(self):
        ''' return list of all stat types based on harvest_config '''
        return self.stats
        
    def get_variables(self):
        ''' return list of all variable types based on harvest_config'''
        return self.variables
        
@dataclass
class LogIncHv(object):
    """ Harvester dataclass used to parse increment descriptive
        statistics stored in log files
    
        Parameters:
        -----------
        config: IncCfg object containing information used to determine
                which variable to parse the log file
        Methods:
        --------
        get_data: parse descriptive statistics from log files based on input
                  config file
    
                  returns a list of tuples containing specific data
    """
    config: LogIncCfg = field(default_factory=LogIncCfg)
    
    def get_data(self):
        """ Harvests increment descriptive statistics.
            Input file is a log text file.
        
            returns harvested_data, a list of HarvestData tuples
        """
        harvested_data = []        
        # read in file
        # find section for statistics
        # read values
        
        with open(self.config.harvest_filename) as log_file:
            lines = log_file.readlines()
        
        for i, line in enumerate(lines):
            """ The outer most loop iterates through each line
            """                
            for j, variable in enumerate(self.config.get_variables()):
                """ The first nested loop iterates through each requested variable
                """
                if variable == 'u_inc' or variable == 'v_inc':
                    """ Because both atmosphere and ocean wind speed
                        increments (u_inc and v_inc) are named the 
                        same, must append these variable names based on 
                        if they are relevant for the atmosphere 
                        ("_atm") or ocean ("_ocn") metrics
                    """
                    if self.config.harvest_filename.split('_')[-2] == 'atm':
                        varname_out = variable + "_atm"
                    elif self.config.harvest_filename.split('_')[-2] == 'ocn':
                        varname_out = variable + "_ocn"
                
                else:
                    varname_out = variable
                        
                for k, statistic in enumerate(self.config.get_stats()):
                    """ The second nested loop iterates through each requested statistic
                    """
                    if line.split(',')[1][1:] == variable:
                        """ harvest data
                        """        
                        if statistic == VALID_STATISTICS[0]:
                            """ harvest mean
                            """ 
                            harvest_tuple = HarvestedData(
                                self.config.harvest_filename,
                                self.config.cycletime,
                                statistic,
                                varname_out,
                                float(line.split(',')[-2]), # value
                                'unspecified', # units
                                )
                        elif statistic == VALID_STATISTICS[1]:
                            """ harvest RMS
                            """
                            harvest_tuple = HarvestedData(
                                self.config.harvest_filename,
                                self.config.cycletime,
                                statistic,
                                varname_out,
                                float(line.split(',')[-1]), # value
                                'unspecified', # units
                                )
                        else:
                            harvest_tuple = ()
                        
                        if len(harvest_tuple) == N_TUPLE_ENTRIES:
                            harvested_data.append(harvest_tuple)
    
        return harvested_data