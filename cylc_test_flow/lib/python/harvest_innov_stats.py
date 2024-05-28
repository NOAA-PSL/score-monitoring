"""
Copyright 2022 NOAA
All rights reserved.

Collection of methods to coordinate harvesting of innovation statistics
created by the UFS-RNR workflow.  The harvested statistics will be inserted
into the UFS-RNR centralized database for easy access at any later time.

"""
from collections import namedtuple
import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import pprint
import traceback

import time_utils
from time_utils import DateRange
from score_hv.harvester_base import harvest
from expt_metrics import ExptMetricInputData, ExptMetricRequest
from db_action_response import DbActionResponse

# import aws_s3_interface as s3
# from aws_s3_interface import AwsS3CommandRawResponse
DEFAULT_OUTPUT_FORMAT = 'pandas_dataframe'
DEFAULT_REGIONS = {
    'equatorial': {
        'lat_min': -5.0,
        'lat_max': 5.0
    },
    'global': {
        'lat_min': -90.0,
        'lat_max': 90.0
    },
    'north_hemis': {
        'lat_min': 20.0,
        'lat_max': 60.0
    },
    'tropics': {
        'lat_min': -20.0,
        'lat_max': 20.0
    },
    'south_hemis': {
        'lat_min': -60.0,
        'lat_max': -20.0
    },
    'south_hemis': {
        'lat_min': -60.0,
        'lat_max': -20.0
    },
}


SCORE_HV_DEFAULT_CONFIG_DICT = {
    'harvester_name': None,
    'file_meta': {
        'filepath': None,
        'cycletime_str': '%Y%m%d%H',
        'cycle': None,
        'filename_str': None
    },
    'stats': None,
    'metrics': None,
    'elevation_unit': None,
    'regions': DEFAULT_REGIONS,
    'output_format': DEFAULT_OUTPUT_FORMAT
}


@dataclass
class FileData:
    file_dict: dict
    cycles: list = field(default_factory=list, init=False)
    filepath_frmt_str: str = field(default_factory=str, init=False)
    filename_frmt_str: str = field(default_factory=str, init=False)
    harvester: str = field(default_factory=str, init=False)
    metrics: list[str] = field(init=False)
    stats: list[str] = field(init=False)

    def __post_init__(self):
        self.cycles = self.file_dict.get('cycles')
        print(f'self.cycles: {self.cycles}')
        self.filepath_frmt_str = self.file_dict.get('filepath')
        self.filename_frmt_str = self.file_dict.get('filename')
        self.harvester = self.file_dict.get('harvester')
        self.metrics = self.file_dict.get('metrics')
        self.stats = self.file_dict.get('stats')
        self.elevation_unit = self.file_dict.get('elevation_unit')

    
    def get_filename(self, cycle_time):
        try:
            self.filename = datetime.strftime(cycle_time, file_format_str)
        except Exception as err:
            trcbk = traceback.format_exc()
            msg = f'Problem formatting filename: {file_format_str}, ' \
                f'cycle_time: {self.cycle_time}, err: {trcbk}'
            print(f'{msg}')

        return self.filename


@dataclass
class HarvestInnovStatsRequest(object):
    config_dict: dict
    date_range: DateRange = field(init=False)
    hv_files: list = field(default_factory=list, init=False)
    output_format: str = field(default_factory=str, init=False)
    datetime_str: str = field(default_factory=str, init=False)
    expt_name: str = field(default_factory=str, init=False)
    expt_wallclk_strt: str = field(default_factory=str, init=False)

    def __post_init__(self):
        date_range_dict = self.config_dict.get('date_range')
        self.datetime_str = date_range_dict.get('datetime_str')
        start_str = date_range_dict.get('start')
        end_str = date_range_dict.get('end')
        try:
            start = datetime.strptime(start_str, self.datetime_str)
            end = datetime.strptime(end_str, self.datetime_str)
        except Exception as err:
            trcbk = traceback.format_exc()
            msg = f'Problem parsing date range: {date_range_dict}, ' \
                f'err: {trcbk}'
            print(f'{msg}')
            raise ValueError(msg) from err

        self.date_range = DateRange(start, end)

        file_list = self.config_dict.get('files')
        if not isinstance(file_list, list):
            trcbk = traceback.format_exc()
            msg = 'No file list key found or invalid type: ' \
                f'files: {files}, type(files): {type(files)}, err: {trcbk}'
            raise TypeError(msg)

        for file_dict in file_list:
            if not isinstance(file_dict, dict):
                trcbk = traceback.format_exc()
                msg = 'No file list key found or invalid type: ' \
                    f'files: {files}, type(files): {type(files)}, err: {trcbk}'
                raise TypeError(msg)

            self.hv_files.append(FileData(file_dict))
        
        print(f'hv_files: {self.hv_files}')
        self.output_format = self.config_dict.get('output_format')
        self.expt_name = self.config_dict.get('expt_name')
        self.expt_wallclk_strt = self.config_dict.get('expt_wallclk_strt')


    def submit(self):
        
        master_list = []
        n_hours = 6
        n_days = 0

        finished = False
        loop_count = 0
        details = {}
        error_msg = ""
        success = True
        messages = ""
        while not finished:
            print(
                f'loop {loop_count} of while loop, finished: {finished}')
            loop_count += 1

            for file_dict in self.hv_files:

                cycle_seconds = self.date_range.cycle_seconds
                if not cycle_seconds in file_dict.cycles:
                    continue
                
                cycle_valid_time = self.date_range.current
                cycle_valid_time += timedelta(hours=n_hours)
                # get file meta
                harvest_config = {
                    'harvester_name': file_dict.harvester,
                    'file_meta': {
                        'filepath_format_str': file_dict.filepath_frmt_str,
                        'filename_format_str': file_dict.filename_frmt_str,
                        'cycletime': self.date_range.current
                    },
                    'stats': file_dict.stats,
                    'metrics': file_dict.metrics,
                    'elevation_unit': file_dict.elevation_unit,
                    'output_format': self.output_format
                }

                print(f'harvest config: {harvest_config}')
                harvested_data = harvest(harvest_config)

                # harvest data from diagnostics file
                # if there are more than 0 rows, stuff the rows into the
                # database

                expt_metrics = []

                print(f'harvested_data: type: {type(harvested_data)}')
                for row in harvested_data:
                    item = ExptMetricInputData(
                        row.name,
                        row.region_name,
                        row.elevation,
                        row.elevation_unit,
                        row.value,
                        row.cycletime, 
                        None,
                        None
                    )

                    expt_metrics.append(item)

                request_dict = {
                    'name': 'expt_metrics',
                    'method': 'PUT',
                    'body': {
                        'expt_name': self.expt_name,
                        'expt_wallclock_start': self.expt_wallclk_strt,
                        'metrics': expt_metrics,
                        'datestr_format': self.datetime_str
                    }
                }

                emr = ExptMetricRequest(request_dict)
                # exit()
                result = emr.submit()
                details[loop_count] = result.details
                messages += result.message
                success = (success and result.success)
                error_msg += result.errors

            self.date_range.increment(days=n_days, hours=n_hours)

            if self.date_range.at_end():
                finished = True
                
        #add db action response 
        response = DbActionResponse(
            self.config_dict,
            success,
            messages,
            details,
            error_msg
        ) 

        print(f'response: {response}')
        return response 
        
