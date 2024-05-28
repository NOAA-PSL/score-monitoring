"""
Copyright NOAA 2024
All rights reserved.

Collection of methods to facilitate handling of requests for array based experiment metrics.
Will also interact with the experiments, regions, array metric types, and sat meta tables. 
"""

from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
import math
import traceback

import numpy as np
import psycopg2
from pandas import DataFrame
from sqlalchemy import and_, or_, not_

from db_action_response import DbActionResponse
import score_table_models as stm
from score_table_models import Experiment as exp
from score_table_models import ExptArrayMetric as ex_arr_mt
from score_table_models import ArrayMetricType as amt
from score_table_models import SatMeta as sm
from score_table_models import Region as rgs
from score_table_models import InstrumentMeta as im
from experiments import ExperimentRequest
from sat_meta import SatMetaRequest
import regions as rg
import array_metric_types as amts
import time_utils
import db_utils

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
psycopg2.extensions.register_adapter(np.float32, psycopg2._psycopg.AsIs)

ExptArrayMetricInputData = namedtuple(
    'ExptArrayMetricInputData',
    [
        'name',
        'region_name',
        'value',
        'assimilated',
        'time_valid',
        'forecast_hour',
        'ensemble_member',
        'sat_meta_name',
        'sat_id',
        'sat_name',
        'sat_short_name',
    ],
) 

ExptArrayMetricsData = namedtuple(
    'ExptArrayMetricsData',
    [
        'id',
        'value',
        'assimilated',
        'time_valid',
        'forecast_hour',
        'ensemble_member',
        'expt_id',
        'expt_name',
        'wallclock_start',
        'metric_id',
        'metric_long_name',
        'metric_type',
        'metric_unit',
        'metric_stat_type',
        'metric_obs_platform',
        'metric_instrument_meta_id',
        'metric_instrument_name',
        'array_coord_labels',
        'array_coord_units',
        'array_index_values',
        'array_dimensions',
        'region_id',
        'region',
        'sat_meta_id',
        'sat_meta_name',
        'sat_id',
        'sat_name',
        'sat_short_name',
        'created_at'
    ],
)

class ExptArrayMetricsError(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message
    
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
        msg = f'string values must be a list, was: {type(values)}'
        raise TypeError(msg)
    
    for value in values:
        if not isinstance(value, str):
            msg = 'all values must be string type - value: ' \
                f'{value} is type: {type(value)}'
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

def get_boolean_filter(filter_dict, cls, key, constructed_filter):
    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filters, must be \'dict\', was ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)

    print(f'Column \'{key}\' is of type {type(getattr(cls, key).type)}.')
    bool_flt = filter_dict.get(key)

    if bool_flt is None:
        print(f'No \'{key}\' filter detected')
        return constructed_filter

    constructed_filter[f'{cls.__name__}.{key}'] = ( getattr(cls, key) == bool_flt )
    
    return constructed_filter

def get_int_filter(filters, cls, key, constructed_filter):
    if not isinstance(filters, dict):
        msg = f'Invalid type for filters, must be \'dict\', was ' \
            f'type: {type(filters)}'
        raise TypeError(msg)

    print(f'Column \'{key}\' is of type {type(getattr(cls, key).type)}.')
    int_flt = filters.get(key)

    if int_flt is None:
        print(f'No \'{key}\' filter detected')
    else:
        constructed_filter[f'{cls.__name__}.{key}'] = ( getattr(cls, key) == int_flt )
    
    return constructed_filter

def get_experiments_filter(filter_dict, constructed_filter):
    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filter, must be \'dict\', was ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)
    
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

def get_array_metric_types_filter(filter_dict, constructed_filter):
    if filter_dict is None:
        return constructed_filter
    
    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filter, must be \'dict\', was ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)
    
    if not isinstance(constructed_filter, dict):
        msg = 'Invalid type for constructed_filter, must be \'dict\', ' \
            f'was type: {type(filter_dict)}'
        raise TypeError(msg)

    constructed_filter = get_string_filter(filter_dict, amt, 'name', constructed_filter, 'name')
    
    constructed_filter = get_string_filter(filter_dict, amt, 'long_name', constructed_filter, 'long_name')
    
    constructed_filter = get_string_filter(filter_dict, amt, 'obs_platform', constructed_filter, 'obs_platform')

    constructed_filter = get_string_filter(filter_dict, amt, 'measurement_type', constructed_filter, 'measurement_type')

    constructed_filter = get_string_filter(filter_dict, amt, 'measurement_units', constructed_filter, 'measurement_units')

    constructed_filter = get_string_filter(filter_dict, amt, 'stat_type', constructed_filter, 'stat_type')
    
    constructed_filter = get_string_filter(filter_dict, im, 'name', constructed_filter, 'instrument_meta_name')

    return constructed_filter   

def get_regions_filter(filter_dict, constructed_filter):
    if filter_dict is None:
        return constructed_filter

    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filter, must be \'dict\', was ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)
    
    if not isinstance(constructed_filter, dict):
        msg = 'Invalid type for constructed_filter, must be \'dict\', ' \
            f'was type: {type(filter_dict)}'
        raise TypeError(msg)

    constructed_filter = get_string_filter(
        filter_dict, rgs, 'name', constructed_filter, 'rgs_name')

    constructed_filter = get_float_filter(filter_dict, rgs, 'min_lat', constructed_filter)

    constructed_filter = get_float_filter(filter_dict, rgs, 'max_lat', constructed_filter)

    constructed_filter = get_float_filter(filter_dict, rgs, 'east_lon', constructed_filter)

    constructed_filter = get_float_filter(filter_dict, rgs, 'west_lon', constructed_filter)

    return constructed_filter

def get_sat_meta_filter(filter_dict, constructed_filter):
    if filter_dict is None:
        return constructed_filter

    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filter, must be \'dict\', was ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)
    
    if not isinstance(constructed_filter, dict):
        msg = 'Invalid type for constructed_filter, must be \'dict\', ' \
            f'was type: {type(filter_dict)}'
        raise TypeError(msg)

    constructed_filter = get_string_filter(filter_dict, sm, 'name', constructed_filter, 'name')

    constructed_filter = get_int_filter(filter_dict, sm, 'sat_id', constructed_filter)

    constructed_filter = get_string_filter(filter_dict, sm, 'sat_name', constructed_filter, 'sat_name')

    constructed_filter = get_string_filter(filter_dict, sm, 'short_name', constructed_filter, 'short_name')

    return constructed_filter


def get_expt_record_id(body):
    expt_name = body.get('expt_name')
    wlclk_strt_str = body.get('expt_wallclock_start')
    
    expt_request = {
        'name': 'experiment',
        'method': db_utils.HTTP_GET,
        'params': {
            'filters': {
                'name': {
                    'exact': expt_name
                },
                'wallclock_start': {
                    'exact': wlclk_strt_str
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
            if records is None:
                msg = 'Request for experiment record did not return a record'
                raise ExptArrayMetricsError(msg)
            record_cnt = records.shape[0]
        else:
            msg = f'Problems encountered requesting experiment data.'
            # create error return db_action_response
            raise ExptArrayMetricsError(msg)
        if record_cnt <= 0:
            msg = 'Request for experiment record did not return a record'
            raise ExptArrayMetricsError(msg)
        
    except Exception as err:
        msg = f'Problems encountered requesting experiment data. err - {err}'
        raise ExptArrayMetricsError(msg)
    
    try:
        experiment_id = records[exp.id.name].iat[0]
    except Exception as err:
        error_msg = f'Problem finding experiment id from record: {records} ' \
            f'- err: {err}'
        print(f'error_msg: {error_msg}')
        raise ExptArrayMetricsError(error_msg) 
        
    return experiment_id

def get_sat_meta_id_from_metric(metric):
    sat_meta_id = -1
    try:    
        sat_meta_name = metric.sat_meta_name
        sat_id = metric.sat_id
        sat_name = metric.sat_name
        sat_short_name = metric.sat_short_name
    except Exception as err:
        print(f'Required sat meta input value not found: {err}')
        return sat_meta_id
    
    if sat_meta_name is None and sat_id is None and sat_name is None and sat_short_name is None:
        return sat_meta_id
    
    sat_meta_request = {
        'name': 'sat_meta',
        'method': db_utils.HTTP_GET,
        'params': {
            'filters': {
                'name': {
                    'exact': sat_meta_name
                },
                'sat_name': {
                    'exact': sat_name
                },
                'short_name': {
                    'exact': sat_short_name
                },
                'sat_id': sat_id
            },
            'record_limit': 1
        }
    }

    print(f'sat_meta_request: {sat_meta_request}')

    smr = SatMetaRequest(sat_meta_request)

    results = smr.submit()
    print(f'results: {results}')

    record_cnt = 0
    try:
        if results.success is True:
            records = results.details.get('records')
            if records is None:
                msg = 'Request for sat meta record did not return a record'
                raise ExptArrayMetricsError(msg)
            record_cnt = records.shape[0]
        else:
            msg = f'Problems encountered requesting sat meta data.'
            # create error return db_action_response
            raise ExptArrayMetricsError(msg)
        if record_cnt <= 0:
            msg = 'Request for sat meta record did not return a record'
            raise ExptArrayMetricsError(msg)
        
    except Exception as err:
        msg = f'Problems encountered requesting sat meta data. err - {err}'
        raise ExptArrayMetricsError(msg) from err
        
    try:
        sat_meta_id = records[sm.id.name].iat[0]
    except Exception as err:
        error_msg = f'Problem finding sat meta id from record: {records} ' \
            f'- err: {err}'
        print(f'error_msg: {error_msg}')
        raise ExptArrayMetricsError(error_msg) from err
    return sat_meta_id

@dataclass 
class ExptArrayMetricRequest:
    request_dict: dict
    method: str = field(default_factory=str, init=False)
    params: dict = field(default_factory=dict, init=False)
    filters: dict = field(default_factory=dict, init=False)
    ordering: list = field(default_factory=list, init=False)
    record_limit: int = field(default_factory=int, init=False)
    body: dict = field(default_factory=dict, init=False)
    array_metric_type_id: int = field(default_factory=int, init=False)
    expt_id: int = field(default_factory=int, init=False)
    response: dict = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.method=self.request_dict.get('method')
        self.params = self.request_dict.get('params')
        self.body = self.request_dict.get('body')
        self.filters = None
        self.ordering = None
        self.record_limit = None
        if self.params is not None:
            self.filters = self.params.get('filters')
            self.ordering = self.params.get('ordering')
            self.record_limit = self.params.get('record_limit')

        if self.method == db_utils.HTTP_PUT:
            try:
                self.expt_id = get_expt_record_id(self.body)
            except Exception as err:
                trcbk = traceback.format_exc()
                error_msg = 'Failed to locate experiment record id necessary to insert experiment array metric record -' \
                    f' trcbk: {trcbk}'
                print(f'Submit PUT error: {error_msg}')
                print(f'Error: {err}')
                return self.failed_request(error_msg)

    def submit(self):
        if self.method == db_utils.HTTP_GET:
            try:
                return self.get_expt_array_metrics()
            except Exception as err:
                trcbk = traceback.format_exc()
                error_msg = 'Failed to get experiment array metric records -' \
                    f' trcbk: {trcbk}'
                print(f'Submit GET error: {error_msg}')
                print(f'Error: {err}')
                return self.failed_request(error_msg)
        elif self.method == db_utils.HTTP_PUT:
            try:
                return self.put_expt_array_metrics()
            except Exception as err:
                trcbk = traceback.format_exc()
                error_msg = 'Failed to insert experiment array metric records -' \
                    f' trcbk: {trcbk}'
                print(f'Submit PUT error: {error_msg}')
                print(f'Error: {err}')
                return self.failed_request(error_msg)
    
    def failed_request(self, error_msg):
        return DbActionResponse(
            request=self.request_dict,
            success=False,
            message='Failed experiment array metrics request.',
            details=None,
            errors=error_msg
        )

    def construct_filters(self, query):
        if not isinstance(self.filters, dict):
            msg = f'Filters must be of the form dict, filters: {type(self.filters)}'
            raise ExptArrayMetricsError(msg)

        constructed_filter = {}

        # filter experiment metrics table for all matching experiments
        constructed_filter = get_experiments_filter(
            self.filters.get('experiment'), constructed_filter)

        # get only those records related to certain experiment
        constructed_filter = get_array_metric_types_filter(
            self.filters.get('array_metric_types'), constructed_filter)
        
        constructed_filter = get_regions_filter(
            self.filters.get('regions'), constructed_filter)
        
        constructed_filter = get_sat_meta_filter(self.filters.get('sat_meta'), constructed_filter)

        constructed_filter = get_time_filter(
            self.filters, ex_arr_mt, 'time_valid', constructed_filter)

        constructed_filter = get_float_filter(self.filters, ex_arr_mt, 'forecast_hour', constructed_filter)

        constructed_filter = get_float_filter(self.filters, ex_arr_mt, 'ensemble_member', constructed_filter)

        constructed_filter = get_boolean_filter(self.filters, ex_arr_mt, 'assimilated', constructed_filter)

        if len(constructed_filter) > 0:
            try:
                for key, value in constructed_filter.items():
                    print(f'adding filter: {value}')
                    query = query.filter(value)
            except Exception as err:
                msg = f'Problems adding filter to query - query: {query}, ' \
                    f'filter: {value}, err: {err}'
                raise ExptArrayMetricsError(msg) from err

        return query

    def parse_metrics_data(self, metrics):
        if not isinstance(metrics, list):
            msg = f'\'array_metrics\' must be a list - was a \'{type(metrics)}\''
            raise ExptArrayMetricsError(msg)
        
        unique_regions = set()
        unique_array_metric_types = set()

        for metric in metrics:
            if not isinstance(metric, ExptArrayMetricInputData):
                msg = 'Each array metric must be a type ' \
                    f'\'{type(ExptArrayMetricInputData)}\' was \'{metric}\''
                print(f'metric: {metric}, msg: {msg}')
                raise ExptArrayMetricsError(msg)
            
            unique_regions.add(metric.region_name)
            unique_array_metric_types.add(metric.name)

        regions = rg.get_regions_from_name_list(list(unique_regions))
        array_metric_types = amts.get_all_array_metric_types()

        rg_df = regions.details.get('records')
        if rg_df.shape[0] != len(unique_regions):
            msg = 'Did not find all unique_regions in regions table ' \
                f'unique_regions: {len(unique_regions)}, found regions: ' \
                f'{rg_df.shape[0]}.'
            print(f'region counts do not match: {msg}')
            raise ExptArrayMetricsError(msg)

        rg_df_dict = dict(zip(rg_df.name, rg_df.id))

        amt_df = array_metric_types.details.get('records')
        amt_df_nm_id = amt_df[['id', 'name']].copy()
        amt_df_dict = dict(zip(amt_df_nm_id.name, amt_df_nm_id.id))

        records = []
        for row in metrics:
            
            value = row.value
            sat_meta_id = get_sat_meta_id_from_metric(row)
            sat_meta_input_id = sat_meta_id if sat_meta_id > 0 else None
            
            item = ex_arr_mt(
                experiment_id=self.expt_id,
                array_metric_type_id=amt_df_dict[row.name],
                region_id=rg_df_dict[row.region_name],
                sat_meta_id=sat_meta_input_id,
                value=value,
                assimilated=row.assimilated,
                time_valid=row.time_valid,
                forecast_hour=row.forecast_hour,
                ensemble_member=row.ensemble_member
            )

            records.append(item)

        return records
    
    def get_expt_array_metrics_from_body(self, body):
        if not isinstance(body, dict):
            error_msg = 'The \'body\' key must be a type dict, was ' \
                f'{type(body)}'
            print(f'Array Metrics key not found: {error_msg}')
            raise ExptArrayMetricsError(error_msg)
        
        array_metrics = body.get('array_metrics')
        parsed_array_metrics = self.parse_metrics_data(array_metrics)

        return parsed_array_metrics
    
    def put_expt_array_metrics(self):
        records = self.get_expt_array_metrics_from_body(self.body)
        session = stm.get_session()


        if len(records) > 0:
            #This section of print statements can be uncommented for debugging
            #Otherwise this is going to cause way too much output 
            # for record in records:
            #     msg = f'record.experiment_id: {record.expt_id}, '
            #     msg += f'record.array_metric_type_id: {record.array_metric_type_id}, '
            #     msg += f'record.region_id: {record.region_id}, '
            #     msg += f'record.value: {record.value}, '
            #     msg += f'record.assimilated: {record.assimilated}, '
            #     msg += f'record.time_valid: {record.time_valid}, '
            #     msg += f'record.forecast_hour: {record.forecast_hour}, '
            #     msg += f'record.ensemble_member: {record.ensemble_member}, '
            #     msg += f'record.created_at: {record.created_at}'
            #     print(f'record: {msg}')

            session.bulk_save_objects(records)
            session.commit()
            session.close()

        else:
            return self.failed_request('No expt array metric records were discovered to be inserted')

        return DbActionResponse(
            request=self.request_dict,
            success=True,
            message="Attempt to insert expt array metrics SUCCEEDED",
            details=records,
            errors=None
        )

    def get_expt_array_metrics(self):
        session = stm.get_session()

        q = session.query(
            ex_arr_mt
        ).join(
            exp, ex_arr_mt.experiment
        ).join(
            rgs, ex_arr_mt.region
        ).outerjoin(
            sm, ex_arr_mt.sat_meta
        ).join(
            amt, ex_arr_mt.array_metric_type
        ).outerjoin(
            im, amt.instrument_meta
        )

        q = self.construct_filters(q)

        column_ordering = db_utils.build_column_ordering(ex_arr_mt, self.ordering)
        if column_ordering is not None and len(column_ordering) > 0:
            for ordering_item in column_ordering:
                q = q.order_by(ordering_item)

        array_metrics = q.all()

        parsed_metrics = []
        for metric in array_metrics:
            #handle potential nulls from outer joins
            sat_meta_id=None
            sat_meta_name=None
            sat_id=None
            sat_name=None
            sat_short_name=None
            metric_instrument_name=None
            if metric.sat_meta is not None:
                sat_meta_id=metric.sat_meta.id
                sat_meta_name=metric.sat_meta.name
                sat_id=metric.sat_meta.sat_id
                sat_name=metric.sat_meta.sat_name
                sat_short_name=metric.sat_meta.short_name
            if metric.array_metric_type.instrument_meta is not None:
                metric_instrument_name=metric.array_metric_type.instrument_meta.name

            record = ExptArrayMetricsData(
                id=metric.id,
                value=metric.value,
                assimilated=metric.assimilated,
                time_valid=metric.time_valid,
                forecast_hour=metric.forecast_hour,
                ensemble_member=metric.ensemble_member,
                expt_id=metric.experiment.id,
                expt_name=metric.experiment.name,
                wallclock_start=metric.experiment.wallclock_start,
                metric_id=metric.array_metric_type.id,
                metric_long_name=metric.array_metric_type.long_name,
                metric_type=metric.array_metric_type.measurement_type,
                metric_unit=metric.array_metric_type.measurement_units,
                metric_stat_type=metric.array_metric_type.stat_type,
                metric_instrument_meta_id=metric.array_metric_type.instrument_meta_id,
                metric_instrument_name=metric_instrument_name,
                metric_obs_platform=metric.array_metric_type.obs_platform,
                array_coord_labels=metric.array_metric_type.array_coord_labels,
                array_coord_units=metric.array_metric_type.array_coord_units,
                array_index_values=metric.array_metric_type.array_index_values,
                array_dimensions=metric.array_metric_type.array_dimensions,
                region_id=metric.region.id,
                region=metric.region.name,
                sat_meta_id=sat_meta_id,
                sat_meta_name=sat_meta_name,
                sat_id=sat_id,
                sat_name=sat_name,
                sat_short_name=sat_short_name,
                created_at=metric.created_at
            )
           
            parsed_metrics.append(record)
        
        try:
            arr_metrics_df = DataFrame(
                parsed_metrics,
                columns=ExptArrayMetricsData._fields
            )
        except Exception as err:
            trcbk = traceback.format_exc()
            msg = f'Problem casting array exeriment metrics query output into pandas ' \
                f'DataFrame - err: {trcbk}'
            raise TypeError(msg) from err
        
        unique_metrics = self.remove_metric_duplicates(arr_metrics_df)

        results = DataFrame()   
        error_msg = None
        record_count = 0
        try:
            if len(parsed_metrics) > 0:
                results = unique_metrics    
        except Exception as err:
            message = 'Request for experiment array metric records FAILED'
            trcbk = traceback.format_exc()
            error_msg = f'Failed to get any experiment array metrics - err: {trcbk}'
            print(f'error_msg: {error_msg}')
        else:
            message = 'Request for experiment array metrics SUCCEEDED'
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

        session.close()
        return response

    def remove_metric_duplicates(self, m_df):
        
        start_records = m_df.shape[0]
        print(f'starting records: {start_records}')

        try:

            uf = m_df.sort_values(
                'created_at'
            ).drop_duplicates(
                [
                    'time_valid',
                    'forecast_hour',
                    'ensemble_member',
                    'expt_id',
                    'metric_id',
                    'region_id',
                ],
                keep='last'
            )

        except Exception as err:
            trcbk = traceback.format_exc()
            msg = f'Failed to drop duplicates - err: {trcbk}'
            print(err)
            raise ValueError(msg)
        
        end_records = uf.shape[0]
        print(f'ending records: {end_records}')
        return uf                   