"""
Copyright 2023 NOAA
All rights reserved.

Collection of methods to facilitate insertion/selection of experiment
file counts.

"""
from collections import namedtuple
import copy
from dataclasses import dataclass, field
from datetime import datetime
import json
import math
import pprint
import traceback

import numpy as np
from psycopg2.extensions import register_adapter, AsIs
from sqlalchemy import Integer, String, Boolean, DateTime, Float
import psycopg2
import pandas as pd
from pandas import DataFrame
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect
from sqlalchemy import and_, or_, not_
from sqlalchemy import asc, desc
from sqlalchemy.sql import func
from sqlalchemy.orm import joinedload


from db_action_response import DbActionResponse
import score_table_models as stm
from score_table_models import Experiment as exp
from score_table_models import FileType as ft
from score_table_models import StorageLocation as sl
from score_table_models import ExptStoredFileCount as esfc 
from experiments import Experiment, ExperimentData
from experiments import ExperimentRequest
from file_types import FileTypeRequest
from storage_locations import StorageLocationRequest
import regions as rg
import metric_types as mt
import time_utils
import db_utils

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
psycopg2.extensions.register_adapter(np.float32, psycopg2._psycopg.AsIs)

ExptFileCountData = namedtuple(
    'ExptFileCountData',
    [
        'id',
        'count',
        'folder_path',
        'cycle',
        'time_valid',
        'forecast_hour',
        'file_size_bytes',
        'experiment_id',
        'experiment_name',
        'wallclock_start',
        'file_type_id',
        'file_type_name',
        'storage_location_id',
        'storage_location_name',
        'created_at'
    ]
)

ExptFileCountInputData = namedtuple(
    'ExptFileCountInputData',
    [
        'count',
        'folder_path',
        'cycle',
        'time_valid',
        'forecast_hour',
        'file_size_bytes',
        'experiment_id',
        'file_type_id',
        'storage_location_id',
    ]
)

class ExptFileCountsError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

    
@dataclass
class ExptFileCount:
    '''class for storing experiment file count values'''
    count: int
    folder_path: String
    cycle: datetime
    time_valid: datetime
    forecast_hour: float 
    file_size_bytes: int
    experiment_id: int
    file_type_id: int
    storage_location_id: int
    expt_file_count_data: ExptFileCountInputData = field(init=False)

    def __post_init__(self):
        self.expt_file_count_data = ExptFileCountInputData(
            self.count,
            self.folder_path,
            self.cycle,
            self.time_valid,
            self.forecast_hour,
            self.file_size_bytes,
            self.experiment_id,
            self.file_type_id,
            self.storage_location_id
        )
    
    def __repr__(self):
        return f'expt_file_count_data: {self.expt_file_count_data}'
    
    def get_expt_file_count_data(self):
        return self.expt_file_count_data
    
def get_file_count_from_body(body):
    if not isinstance(body, dict):
        msg = 'The \'body\' key must be a type dict, was ' \
            f'{type(body)}'
        raise TypeError(msg)
    
    experiment_id = get_experiment_id(body.get('experiment_name'), body.get('wallclock_start'))
    file_type_id = get_file_type_id(body.get('file_type_name'))
    storage_location_id = get_storage_location_id(body.get('bucket_name'), body.get('platform'), body.get('key')) 

    file_count = ExptFileCount(
        body.get('count'),
        body.get('folder_path'),
        body.get('cycle'),
        body.get('time_valid'),
        body.get('forecast_hour'),
        body.get('file_size_bytes'),
        experiment_id,
        file_type_id,
        storage_location_id
    )

    return file_count

def get_time_filter(filter_dict, cls, key, constructed_filter):
    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filters, must be \'dict\', was ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)

    value = filter_dict.get(key)
    if value is None:
        print(f'No \'{key}\' filter detected')
        return constructed_filter

    exact_datetime = time_utils.get_time(value.get(db_utils.EXACT_DATETIME))

    if exact_datetime is not None:
        constructed_filter[f'{cls.__name__}.{key}'] = (
            getattr(cls, key) == exact_datetime
        )
        return constructed_filter

    from_datetime = time_utils.get_time(value.get(db_utils.FROM_DATETIME))
    to_datetime = time_utils.get_time(value.get(db_utils.TO_DATETIME))

    if from_datetime is not None and to_datetime is not None:
        if to_datetime < from_datetime:
            raise ValueError('\'from\' must be older than \'to\'')
        
        constructed_filter[f'{cls.__name__}.{key}'] = and_(
            getattr(cls, key) >= from_datetime,
            getattr(cls, key) <= to_datetime
        )
    elif from_datetime is not None:
        constructed_filter[f'{cls.__name__}.{key}'] = (
            getattr(cls, key) >= from_datetime
        )
    elif to_datetime is not None:
        constructed_filter[f'{cls.__name__}.{key}'] = (
            getattr(cls, key) <= to_datetime
        )

    return constructed_filter

def validate_list_of_strings(values):
    if isinstance(values, str):
        val_list = []
        val_list.append(values)
        return val_list

    if not isinstance(values, list):
        msg = f'string values must be a list - was: {type(values)}'
        raise TypeError(msg)
    
    for value in values:
        if not isinstance(value, str):
            msg = 'all values must be string type - value: ' \
                f'{value} was type: {type(value)}'
            raise TypeError(msg)
    
    return values

def get_string_filter(filter_dict, cls, key, constructed_filter, key_name):
    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filters, must be \'dict\', was ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)

    print(f'Column \'{key}\' is of type {type(getattr(cls, key).type)}.')
    string_flt = filter_dict.get(key_name)
    print(f'string_flt: {string_flt}')

    if string_flt is None:
        print(f'No \'{key}\' filter detected')
        return constructed_filter

    like_filter = string_flt.get('like')
    # prefer like search over exact match if both exist
    if like_filter is not None:
        constructed_filter[f'{cls.__name__}.{key}'] = (getattr(cls, key).like(like_filter))
        return constructed_filter

    exact_match_filter = validate_list_of_strings(string_flt.get('exact'))
    if exact_match_filter is not None:
        constructed_filter[f'{cls.__name__}.{key}'] = (getattr(cls, key).in_(exact_match_filter))

    return constructed_filter

def get_float_filter(filter_dict, cls, key, constructed_filter):
    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filters, must be \'dict\', was ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)

    print(f'Column \'{key}\' is of type {type(getattr(cls, key).type)}.')
    float_flt = filter_dict.get(key)

    if float_flt is None:
        print(f'No \'{key}\' filter detected')
        return constructed_filter

    constructed_filter[f'{cls.__name__}.{key}'] = ( getattr(cls, key) == float_flt )
    
    return constructed_filter

def get_experiments_filter(filter_dict, constructed_filter):
    if filter_dict is None:
        print('No experiment filters provided')
        return constructed_filter
    
    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for experiment filter, must be \'dict\', was ' \
            f'type: {type(filter_dict)}. No experiment filters will be added.'
        print(msg)
        return constructed_filter
    
    if not isinstance(constructed_filter, dict):
        msg = 'Invalid type for constructed_filter, must be \'dict\', ' \
            f'was type: {type(filter_dict)}'
        raise TypeError(msg)   
    
    constructed_filter = get_string_filter(
        filter_dict, exp, 'name', constructed_filter, 'experiment_name')
    
    constructed_filter = get_time_filter(
        filter_dict, exp, 'cycle_start', constructed_filter)

    constructed_filter = get_time_filter(
        filter_dict, exp, 'cycle_stop', constructed_filter)

    constructed_filter = get_time_filter(
        filter_dict, exp, 'wallclock_start', constructed_filter)
    
    return constructed_filter

def get_file_types_filter(filter_dict, constructed_filter):
    if filter_dict is None:
        print('No file type filters provided')
        return constructed_filter
    
    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for file type filter, must be \'dict\', was ' \
            f'type: {type(filter_dict)}. No file type filters will be added.'
        print(msg)
        return constructed_filter
    
    if not isinstance(constructed_filter, dict):
        msg = 'Invalid type for constructed_filter, must be \'dict\', ' \
            f'was type: {type(filter_dict)}'
        raise TypeError(msg)   
    
    constructed_filter = get_string_filter(
        filter_dict, ft, 'name', constructed_filter, 'file_type_name')

    constructed_filter = get_string_filter(
        filter_dict, ft, 'file_template', constructed_filter, 'file_template')

    constructed_filter = get_string_filter(
        filter_dict, ft, 'file_format', constructed_filter, 'file_format')
    
    return constructed_filter

def get_storage_locations_filter(filter_dict, constructed_filter):
    if filter_dict is None:
        print('No storage location filters provided')
        return constructed_filter
    
    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for storage location filter, must be \'dict\', was ' \
            f'type: {type(filter_dict)}. No storage location filters will be added.'
        print(msg)
        return constructed_filter
    
    if not isinstance(constructed_filter, dict):
        msg = 'Invalid type for constructed_filter, must be \'dict\', ' \
            f'was type: {type(filter_dict)}'
        raise TypeError(msg)   
    
    constructed_filter = get_string_filter(
        filter_dict, sl, 'name', constructed_filter, 'storage_location_name')

    constructed_filter = get_string_filter(
        filter_dict, sl, 'bucket_name', constructed_filter, 'bucket_name')

    constructed_filter = get_string_filter(
        filter_dict, sl, 'platform', constructed_filter, 'platform')

    constructed_filter = get_string_filter(
        filter_dict, sl, 'platform_region', constructed_filter, 'platform_region')
    
    constructed_filter = get_string_filter(
        filter_dict, sl, 'key', constructed_filter, 'key')
    
    return constructed_filter


def get_experiment_id(experiment_name, wallclock_start):
    expt_request = {
        'name': 'experiment',
        'method': db_utils.HTTP_GET,
        'params': {
            'filters': {
                'name': {
                    'exact': experiment_name
                },
                'wallclock_start': {
                    'exact': wallclock_start
                },
            },
            'ordering': [
                {'name': 'wallclock_start', 'order_by': 'desc'}
            ],
            'record_limit': 1
        }
    }
    print(f'expt_request: {expt_request}')
    er = ExperimentRequest(expt_request)
    results = er.submit()
    print(f'results: {results}')

    record_cnt = 0
    try:
        if results.success is True:
            records = results.details.get('records')
            record_cnt = records.shape[0]
        else:
            msg = f'Problems encountered requesting experiment data.'
            # create error return db_action_response
            raise ExptFileCountsError(msg)
        if record_cnt <= 0:
            msg = 'Request for experiment record did not return a record'
            raise ExptFileCountsError(msg)
        
    except Exception as err:
        msg = f'Problems encountered requesting experiment data. err - {err}'
        raise ExptFileCountsError(msg)
    
    try:
        # record = records[0]
        expt_id = records[exp.id.name].iat[0]
    except Exception as err:
        error_msg = f'Problem finding experiment id from request: {expt_request} '\
            f'- err: {err}'
        print(f'error_msg: {error_msg}')
        raise ExptFileCountsError(error_msg)
    return expt_id


def get_file_type_id(file_type_name):
    type_request = {
        'name': 'file_types',
        'method': db_utils.HTTP_GET,
        'params': {
            'filters': {
                'name': {
                    'exact': file_type_name
                },
            },
            'record_limit': 1
        }
    }
    print(f'type_request: {type_request}')
    ftr = FileTypeRequest(type_request)
    results = ftr.submit()
    print(f'results: {results}')
    
    record_cnt = 0
    try:
        if results.success is True:
            records = results.details.get('records')
            record_cnt = records.shape[0]
        else:
            msg = f'Problems encountered requesting file type data.'
            raise ExptFileCountsError(msg)
        if record_cnt <= 0:
            msg = 'Request for file type record did not return a record'
            raise ExptFileCountsError(msg)
        
    except Exception as err:
        msg = f'Problems encountered requesting file type data. err - {err}'
        raise ExptFileCountsError(msg)
    
    try:
        file_type_id = records[ft.id.name].iat[0]
    except Exception as err:
        error_msg = f'Problem finding file type id from request: {type_request} '\
            f'- err: {err}'
        print(f'error_msg: {error_msg}')
        raise ExptFileCountsError(error_msg)

    return file_type_id

def get_storage_location_id(bucket_name, platform, key):
    storage_loc_request = {
        'name': 'storage_locations',
        'method': db_utils.HTTP_GET,
        'params': {
            'filters': {
                'bucket_name': {
                    'exact': bucket_name
                },
                'platform': {
                    'exact': platform
                },
                'key': {
                    'exact': key
                }
            },
            'record_limit': 1
        }
    }
    print(f'storage_loc_request: {storage_loc_request}')
    slr = StorageLocationRequest(storage_loc_request)
    results = slr.submit()
    print(f'results: {results}')

    record_cnt = 0
    try:
        if results.success is True:
            records = results.details.get('records')
            record_cnt = records.shape[0]
        else:
            msg = f'Problems encountered requesting storage location data.'
            raise ExptFileCountsError(msg)
        if record_cnt <= 0:
            msg = 'Request for storage location record did not return a record'
            raise ExptFileCountsError(msg)
        
    except Exception as err:
        msg = f'Problems encountered requesting storage location data. err - {err}'
        raise ExptFileCountsError(msg)
    
    try:
        storage_loc_id = records[sl.id.name].iat[0]
    except Exception as err:
        error_msg = f'Problem finding storage location id from request: {storage_loc_request} '\
            f'- err: {err}'
        print(f'error_msg: {error_msg}')
        raise ExptFileCountsError(error_msg)

    return storage_loc_id

"""
This class handles interaction requests with the expt_stored_file_counts table.
For calls to the database regarding inputing and reading experiment file counts. 
"""
@dataclass
class ExptFileCountRequest:
    request_dict: dict
    method: str = field(default_factory=str, init=False)
    params: dict = field(default_factory=dict, init=False)
    filters: dict = field(default_factory=dict, init=False)
    ordering: list = field(default_factory=list, init=False)
    record_limit: int = field(default_factory=int, init=False)
    body: dict = field(default_factory=dict, init=False)
    expt_file_count: ExptFileCount = field(init=False)
    expt_file_count_data: namedtuple = field(init=False)
    response: dict = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.method = db_utils.validate_method(self.request_dict.get('method'))
        self.params = self.request_dict.get('params')
        self.body = self.request_dict.get('body')

        if self.method == db_utils.HTTP_PUT:
            self.expt_file_count = get_file_count_from_body(self.body)
            self.expt_file_count_data = self.expt_file_count.get_expt_file_count_data()
            for k, v in zip(
                self.expt_file_count_data._fields, self.expt_file_count_data
            ):
                val = pprint.pformat(v, indent=4)
                print(f'exp_data: k: {k}, v: {val}')
        else:
            print(f'In ExptFileCountRequest - params: {self.params}')
            if isinstance(self.params, dict):
                self.filters = self.params.get('filters')
                self.ordering = self.params.get('ordering')
                self.record_limit = self.params.get('record_limit')

                if not type(self.record_limit) == int or self.record_limit <= 0:
                    self.record_limit = None
            else:
                self.filters = None
                self.ordering = None
                self.record_limit = None
    
    def failed_request(self, error_msg):
        return DbActionResponse(
            request=self.request_dict,
            success=False,
            message= 'Failed file count request.',
            details=None,
            errors=error_msg
        )
    
    def construct_filters(self, filters, query):
        if not isinstance(filters, dict):
            msg = f'Filters must be of the form dict, filters: {type(self.filters)}'
            raise ExptFileCountsError(msg)
        
        constructed_filter = {}

        constructed_filter = get_experiments_filter(
            filters.get('experiment'), constructed_filter)
        
        constructed_filter = get_file_types_filter(
            filters.get('file_types'), constructed_filter)
        
        constructed_filter = get_storage_locations_filter(
            filters.get('storage_locations'), constructed_filter)
        
        constructed_filter = get_float_filter(
            filters,
            esfc,
            'forecast_hour',
            constructed_filter)

        constructed_filter = get_time_filter(
            filters, esfc, 'time_valid', constructed_filter)
        
        constructed_filter = get_float_filter(
            filters,
            esfc,
            'count',
            constructed_filter
        )

        constructed_filter = get_string_filter(
            filters,
            esfc,
            'folder_path',
            constructed_filter,
            'folder_path'
        )

        constructed_filter = get_time_filter(filters, esfc, 'cycle', constructed_filter)

        constructed_filter = get_float_filter(filters, esfc, 'file_size_bytes', constructed_filter)

        if len(constructed_filter) > 0:
            try: 
                for key,value in constructed_filter.items():
                    print(f'adding filter: {value}')
                    query = query.filter(value)
            except Exception as err:
                msg = f'Problems adding filter to query - query: {query}, ' \
                    f'filter: {value}, err: {err}'
                raise ExptFileCountsError(msg) from err 
            
        return query

    def submit(self):
        if self.method == db_utils.HTTP_GET:
            return self.get_expt_file_counts()
        elif self.method == db_utils.HTTP_PUT:
            try:
                return self.put_expt_file_counts()
            except Exception as err:
                error_msg = 'Failed to insert expt file count record -' \
                    f' err: {err}'
                print(f'Submit PUT error: {error_msg}')
                return self.failed_request(error_msg)

    def put_expt_file_counts(self):
        session = stm.get_session()

        insert_stmt = insert(esfc).values(
            count=self.expt_file_count_data.count,
            folder_path=self.expt_file_count_data.folder_path,
            cycle=self.expt_file_count_data.cycle,
            time_valid=self.expt_file_count.time_valid,
            forecast_hour=self.expt_file_count_data.forecast_hour,
            file_size_bytes=self.expt_file_count.file_size_bytes,
            experiment_id=self.expt_file_count_data.experiment_id,
            file_type_id=self.expt_file_count_data.file_type_id,
            storage_location_id=self.expt_file_count_data.storage_location_id,
            created_at=datetime.utcnow()
        ).returning(esfc)
        print(f'insert_stmt: {insert_stmt}')
        result_row = None
        try:
            result = session.execute(insert_stmt)
            session.flush()
            result_row = result.fetchone()
            session.commit()
            session.close()
        except Exception as err:
            message = f'Attempt to insert experiment stored file counts record FAILED'
            error_msg = f'Failed to insert record - err: {err}'
            print(f'error_msg: {error_msg}')
        else:
            message = f'Attempt to insert experiment stored file counts record SUCCEEDED'
            error_msg = None
        
        results = {}
        if result_row is not None:
            results['action'] = db_utils.INSERT
            results['data'] = [result_row._mapping]
            results['id'] = result_row.id

        response = DbActionResponse(
            self.request_dict,
            (error_msg is None),
            message,
            results,
            error_msg
        )

        print(f'response: {response}')
        return response
    
    def get_expt_file_counts(self):
        session = stm.get_session()

        q = session.query(
            esfc
        ).join(
            exp, esfc.experiment
        ).join(
            ft, esfc.file_type
        ).join(
            sl, esfc.storage_location
        )

        print('Before adding filters to the expt file counts request####')
        if self.filters is not None and len(self.filters) > 0:
            q = self.construct_filters(self.filters, q)
        print('After adding filters to the expt file counts request####')

        # add column ordering
        column_ordering = db_utils.build_column_ordering(ft, self.ordering)
        if column_ordering is not None and len(column_ordering) > 0:
            for ordering_item in column_ordering:
                q = q.order_by(ordering_item)

        # limit number of returned records
        if self.record_limit is not None and self.record_limit > 0:
            q = q.limit(self.record_limit)

        file_counts = q.all()

        parsed_counts = []
        for count in file_counts:
            record = ExptFileCountData(
                id=count.id,
                count=count.count, 
                folder_path=count.folder_path,
                cycle=count.cycle,
                time_valid=count.time_valid,
                forecast_hour=count.forecast_hour,
                file_size_bytes=count.file_size_bytes,
                experiment_id=count.experiment.id,
                experiment_name=count.experiment.name,
                wallclock_start=count.experiment.wallclock_start,
                file_type_id=count.file_type.id,
                file_type_name=count.file_type.name,
                storage_location_id=count.storage_location.id,
                storage_location_name=count.storage_location.name,
                created_at=count.created_at
            )
            parsed_counts.append(record)
    

        results = DataFrame()
        error_msg = None
        record_count = 0
        try:
            if len(file_counts) > 0:
                results = DataFrame(parsed_counts, columns = ExptFileCountData._fields)
            
        except Exception as err:
            message = 'Request for expt file counts records FAILED'
            error_msg = f'Failed to get expt file counts records - err: {err}'
        else:
            message = 'Request for expt file counts records SUCCEEDED'
            record_count = len(results.index)
        
        details = {}
        details['record_count'] = record_count

        if record_count > 0:
            details['records'] = results

        response = DbActionResponse(
            self.request_dict,
            (error_msg is None),
            message,
            details,
            error_msg
        )

        print(f'response: {response}')

        return response