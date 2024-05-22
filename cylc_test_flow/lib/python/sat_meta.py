"""
Copyright NOAA 2024.
All rights reserved.

Collection of methods to faciliate interactions with the sat meta table.
"""

from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
from db_action_response import DbActionResponse
import score_table_models as stm
from score_table_models import SatMeta as sm 
import db_utils

import numpy as np
import psycopg2
from pandas import DataFrame
from sqlalchemy.dialects.postgresql import insert

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
psycopg2.extensions.register_adapter(np.float32, psycopg2._psycopg.AsIs)

SatMetaData = namedtuple(
    'SatMetaData',
    [
        'name',
        'sat_id',
        'sat_name',
        'short_name',
    ],
)

@dataclass
class SatMeta:
    '''sat meta data object'''
    name: str
    sat_id: int
    sat_name: str
    short_name: str
    sat_meta_data: SatMetaData = field(init=False)

    def __post_init__(self):
        self.sat_meta_data = SatMetaData(
            self.name,
            self.sat_id,
            self.sat_name,
            self.short_name
        )

    def __repr__(self):
        return f'sat_meta_data: {self.sat_meta_data}'
    
    def get_sat_meta_data(self):
        return self.sat_meta_data

def get_sat_meta_from_body(body):
    if not isinstance(body, dict):
        msg = 'The \'body\' key must be a type dict, was ' \
            f'{type(body)}'
        raise TypeError(msg)
    
    sat_meta = SatMeta(
        name=body.get('name'),
        sat_id=body.get('sat_id'),
        sat_name=body.get('sat_name'),
        short_name=body.get('short_name')
    )
    return sat_meta

def get_string_filter(filters, cls, key, constructed_filter):
    if not isinstance(filters, dict):
        msg = f'Invalid type for filters, must be \'dict\', was ' \
            f'type: {type(filters)}'
        raise TypeError(msg)

    print(f'Column \'{key}\' is of type {type(getattr(cls, key).type)}.')
    string_flt = filters.get(key)

    if string_flt is None:
        print(f'No \'{key}\' filter detected')
        return constructed_filter

    like_filter = string_flt.get('like')
    # prefer like search over exact match if both exist
    if like_filter is not None:
        constructed_filter[key] = (getattr(cls, key).like(like_filter))
        return constructed_filter

    exact_match_filter = string_flt.get('exact')
    if exact_match_filter is not None:
        constructed_filter[key] = (getattr(cls, key) == exact_match_filter)

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
        return constructed_filter

    constructed_filter[key] = ( getattr(cls, key) == int_flt )
    
    return constructed_filter

def construct_filters(filters):
    constructed_filter = {}

    constructed_filter = get_string_filter(filters, sm, 'name', constructed_filter)

    constructed_filter = get_int_filter(filters, sm, 'sat_id', constructed_filter)

    constructed_filter = get_string_filter(filters, sm, 'sat_name', constructed_filter)

    constructed_filter = get_string_filter(filters, sm, 'short_name', constructed_filter)

    return constructed_filter

@dataclass
class SatMetaRequest:
    request_dict: dict
    method: str = field(default_factory=str, init=False)
    params: dict = field(default_factory=dict, init=False)
    filters: dict = field(default_factory=dict, init=False)
    ordering: list = field(default_factory=list, init=False)
    record_limit: int = field(default_factory=int, init=False)
    body: dict = field(default_factory=dict, init=False)
    sat_meta: SatMeta = field(init=False)
    response: dict = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.method = db_utils.validate_method(self.request_dict.get('method'))
        self.params = self.request_dict.get('params')
        self.body = self.request_dict.get('body')
        self.filters = None
        self.ordering = None
        self.record_limit = None

        if self.method == db_utils.HTTP_PUT:
            self.sat_meta = get_sat_meta_from_body(self.body)
        else:
            if isinstance(self.params, dict):
                self.filters = construct_filters(self.params.get('filters'))
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
            message='Failed sat meta request',
            details=None, 
            errors=error_msg
        )

    def submit(self):
        if self.method == db_utils.HTTP_GET:
            return self.get_sat_metas()
        elif self.method == db_utils.HTTP_PUT:
            try:
                return self.put_sat_meta()
            except Exception as err:
                error_msg = 'Failed to insert sat meta record -'\
                    f' err: {err}'
                print(f'Submit PUT sat meta error: {error_msg}')
                return self.failed_request(error_msg)
            
    def put_sat_meta(self):
        session = stm.get_session()

        insert_stmt = insert(sm).values(
            name = self.sat_meta.name,
            sat_id = self.sat_meta.sat_id,
            sat_name = self.sat_meta.sat_name,
            short_name = self.sat_meta.short_name,
            created_at = datetime.utcnow(),
            updated_at = None
        ).returning(sm)
        print(f'insert stmt: {insert_stmt}')

        time_now = datetime.utcnow()

        do_update_stmt = insert_stmt.on_conflict_do_update(
            constraint='unique_sat_meta',
            set_=dict(
                name = self.sat_meta.name,
                updated_at = time_now
            )
        )
        
        print(f'do_update_stmt: {do_update_stmt}')

        try:
            result = session.execute(do_update_stmt)
            session.flush()
            result_row = result.fetchone()
            action = db_utils.INSERT
            if result_row.updated_at is not None:
                action = db_utils.UPDATE

            session.commit()
            session.close()
        except Exception as err:
            message = f'Attempt to INSERT/UPDATE sat meta record FAILED'
            error_msg = f'Failed to insert/update record - err: {err}'
            print(f'error_msg: {error_msg}')
            session.close()
        else:
            message = f'Attempt to {action} sat meta record SUCCEEDED'
            error_msg = None
        
        results = {}
        if result_row is not None:
            results['action'] = action
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
    
    def get_sat_metas(self):
        session = stm.get_session()

        q = session.query(
            sm.id,
            sm.name,
            sm.sat_id,
            sm.sat_name, 
            sm.short_name,
            sm.created_at,
            sm.updated_at
        ).select_from(
            sm
        )

        print('Before adding filters to sat meta request########################')
        if self.filters is not None and len(self.filters) > 0:
            for key, value in self.filters.items():
                q = q.filter(value)
        
        print('After adding filters to sat meta request########################')
        
        # add column ordering
        column_ordering = db_utils.build_column_ordering(sm, self.ordering)
        if column_ordering is not None and len(column_ordering) > 0:
            for ordering_item in column_ordering:
                q = q.order_by(ordering_item)

        # limit number of returned records
        if self.record_limit is not None and self.record_limit > 0:
            q = q.limit(self.record_limit)

        sat_metas = q.all()

        results = DataFrame()
        error_msg = None
        record_count = 0
        try:
            if len(sat_metas) > 0:
                results = DataFrame(sat_metas, columns = sat_metas[0]._fields)
            
        except Exception as err:
            message = 'Request for sat meta records FAILED'
            error_msg = f'Failed to get sat meta records - err: {err}'
        else:
            message = 'Request for sat meta records SUCCEEDED'
            for idx, row in results.iterrows():
                print(f'idx: {idx}, row: {row}')
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