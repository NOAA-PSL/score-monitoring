"""
Copyright 2023 NOAA
All rights reserved.

Collection of methods to coordinate harvesting of metrics via the score-hv
harvester options.  The harvested metrics will be inserted
into the centralized database for easy access at any later time.

"""
from collections import namedtuple
import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import pprint
import traceback
import hv_translator_registry as hvtr 
from db_action_response import DbActionResponse

import time_utils
from time_utils import DateRange
from score_hv.harvester_base import harvest
from expt_metrics import ExptMetricInputData, ExptMetricRequest
from expt_array_metrics import ExptArrayMetricInputData, ExptArrayMetricRequest

@dataclass
class HarvestMetricsRequest(object):
    request_dict: dict
    body: dict = field(default_factory=dict, init=False)
    harvest_config: dict = field(default_factory=dict, init=False)
    expt_name: str = field(default_factory=str, init=False)
    expt_wallclock_start: str = field(default_factory=str, init=False) 
    datetime_str: str = field(default_factory=str, init=False)
    hv_translator: str = field(default_factory=str, init=False)
    is_array: bool = field(default_factory=str, init=False)

    def __post_init__(self):
        self.body = self.request_dict.get('body')
        self.hv_config = self.request_dict.get('harvest_config')
        self.hv_translator = self.request_dict.get('hv_translator')
        self.is_array = self.request_dict.get('is_array')
        if self.is_array is None:
            self.is_array = False

        self.expt_name = self.body.get('expt_name')
        self.expt_wallclock_start = self.body.get('expt_wallclock_start')
        self.datetime_str = self.body.get('datetime_str')

    def failed_request(self, error_msg):
        return DbActionResponse(
            request=self.request_dict,
            success=False,
            message='Failed harvest metric request.',
            details=None,
            errors=error_msg
        )

    def submit(self):
        if self.is_array:
            return self.submit_array_metrics()
        else:
            return self.submit_single_metrics()

    #function for harvesting and saving to expt metrics table
    def submit_single_metrics(self):
        # get harvested data
        print(f'harvest config: {self.hv_config}')
        harvested_data = harvest(self.hv_config)

        expt_metrics = []
        print(f'harvested_data: type: {type(harvested_data)}')
        for row in harvested_data:
            data = ""
            #Call appropriate translator if one is provided
            if self.hv_translator != "":
                try:
                    translator = hvtr.translator_registry.get(self.hv_translator)
                    data = translator.translate(row)
                except Exception as err: 
                    error_message = f'An error occurred when translating the data with translator input {self.hv_translator}. ' \
                          f'Valid translators: {hvtr.valid_translators}. Error: {err}'
                    return self.failed_request(error_message)
            else:
                data = row

            item = ExptMetricInputData(
                data.name,
                data.region_name,
                data.elevation,
                data.elevation_unit,
                data.value,
                data.cycletime,
                data.forecast_hour,
                data.ensemble_member
            )

            expt_metrics.append(item)

        request_dict = {
            'name': 'expt_metrics',
            'method': 'PUT',
            'body': {
                'expt_name': self.expt_name,
                'expt_wallclock_start': self.expt_wallclock_start,
                'metrics': expt_metrics,
                'datestr_format': self.datetime_str
            }
        }

        emr = ExptMetricRequest(request_dict)
        result = emr.submit()
        return result
    
    #function for harvesting and saving values to expt array metrics
    def submit_array_metrics(self):
        # get harvested data
        print(f'harvest config: {self.hv_config}')
        harvested_data = harvest(self.hv_config)

        expt_array_metrics = []
        print(f'harvested_data: type: {type(harvested_data)}')
        for row in harvested_data:
            data = ""
            #Call appropriate translator if one is provided
            if self.hv_translator != "":
                try:
                    translator = hvtr.translator_registry.get(self.hv_translator)
                    data = translator.translate(row)
                except Exception as err: 
                    error_message = f'An error occurred when translating the data with translator input {self.hv_translator}. ' \
                          f'Valid translators: {hvtr.valid_translators}. Error: {err}'
                    return self.failed_request(error_message)
            else:
                data = row

            item = ExptArrayMetricInputData(
                data.name,
                data.region_name,
                data.value,
                data.assimilated,
                data.time_valid,
                data.forecast_hour,
                data.ensemble_member,
                data.sat_meta_name, 
                data.sat_id,
                data.sat_name,
                data.sat_short_name
            )

            expt_array_metrics.append(item)

        request_dict = {
            'name': 'expt_array_metrics',
            'method': 'PUT',
            'body': {
                'expt_name': self.expt_name,
                'expt_wallclock_start': self.expt_wallclock_start,
                'array_metrics': expt_array_metrics,
                'datestr_format': self.datetime_str
            }
        }

        eamr = ExptArrayMetricRequest(request_dict)
        result = eamr.submit()
        return result