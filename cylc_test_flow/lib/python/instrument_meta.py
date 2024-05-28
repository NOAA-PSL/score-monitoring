"""
Copyright NOAA 2024.
All rights reserved.

Collection of methods to faciliate interactions with the instrument meta table.
"""

from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
from db_action_response import DbActionResponse
import score_table_models as stm
from score_table_models import InstrumentMeta as im
import db_utils

import numpy as np
import psycopg2
from pandas import DataFrame

from sqlalchemy.dialects.postgresql import insert

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
psycopg2.extensions.register_adapter(np.float32, psycopg2._psycopg.AsIs)

InstrumentMetaData = namedtuple(
    'InstrumentMetaData',
    [
        'name',
        'num_channels',
        'scan_angle',
    ],
)

@dataclass
class InstrumentMeta:
    '''instrument meta data object'''
    name: str
    num_channels: int
    scan_angle: str
    instrument_meta_data: InstrumentMetaData = field(init=False)

    def __post_init__(self):
        self.instrument_meta_data = InstrumentMetaData(
            self.name,
            self.num_channels,
            self.scan_angle
        )
    
    def __repr__(self):
        return f'instrument_meta_data: {self.instrument_meta_data}'
    
    def get_instrument_meta_data(self):
        return self.instrument_meta_data
    
def get_instrument_meta_from_body(body):
    if not isinstance(body, dict):
        msg = 'The \'body\' key must be a type dict, was ' \
            f'{type(body)}'
        raise TypeError(msg)
    
    instrument_meta = InstrumentMeta(
        name=body.get('name'),
        num_channels=body.get('num_channels'),
        scan_angle=body.get('scan_angle')
    )
    return instrument_meta

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

    constructed_filter = get_string_filter(filters, im, 'name', constructed_filter)
    
    constructed_filter = get_int_filter(filters, im, 'num_channels', constructed_filter)

    constructed_filter = get_string_filter(filters, im, 'scan_angle', constructed_filter)

    return constructed_filter


@dataclass
class InstrumentMetaRequest:
    request_dict: dict
    method: str = field(default_factory=str, init=False)
    params: dict = field(default_factory=dict, init=False)
    filters: dict = field(default_factory=dict, init=False)
    ordering: list = field(default_factory=list, init=False)
    record_limit: int = field(default_factory=int, init=False)
    body: dict = field(default_factory=dict, init=False)
    instrument_meta: InstrumentMeta = field(init=False)
    response: dict = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.method = db_utils.validate_method(self.request_dict.get('method'))
        self.params = self.request_dict.get('params')
        self.body = self.request_dict.get('body')
        self.filters = None
        self.ordering = None
        self.record_limit = None

        if self.method == db_utils.HTTP_PUT:
            self.instrument_meta = get_instrument_meta_from_body(self.body)
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
            message='Failed instrument meta request',
            details=None, 
            errors=error_msg
        )

    def submit(self):
        if self.method == db_utils.HTTP_GET:
            return self.get_instrument_metas()
        elif self.method == db_utils.HTTP_PUT:
            try:
                return self.put_instrument_meta()
            except Exception as err:
                error_msg = 'Failed to insert instrument meta record -'\
                    f' err: {err}'
                print(f'Submit PUT instrument meta error: {error_msg}')
                return self.failed_request(error_msg)

    def put_instrument_meta(self):
        session = stm.get_session()

        insert_stmt = insert(im).values(
            name = self.instrument_meta.name,
            num_channels = self.instrument_meta.num_channels,
            scan_angle = self.instrument_meta.scan_angle,
            created_at = datetime.utcnow(),
            updated_at = None
        ).returning(im)
        print(f'insert stmt: {insert_stmt}')

        time_now = datetime.utcnow()

        do_update_stmt = insert_stmt.on_conflict_do_update(
            constraint='unique_instrument_meta',
            set_=dict(
                num_channels = self.instrument_meta.num_channels,
                scan_angle = self.instrument_meta.scan_angle,
                updated_at = time_now
            )
        )

        print(f'do update stmt: {do_update_stmt}')

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
            message = f'Attempt to INSERT/UPDATE instrument meta record FAILED'
            error_msg = f'Failed to insert/update record - err: {err}'
            print(f'error_msg: {error_msg}')
            session.close()
        else:
            message = f'Attempt to {action} instrument meta record SUCCEEDED'
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
    

    def get_instrument_metas(self):
        session = stm.get_session()

        q = session.query(
            im.id,
            im.name,
            im.num_channels,
            im.scan_angle,
            im.created_at,
            im.updated_at
        ).select_from(
            im
        )

        print('Before adding filters to instrument meta request########################')
        if self.filters is not None and len(self.filters) > 0:
            for key, value in self.filters.items():
                q = q.filter(value)
        
        print('After adding filters to instrument meta request########################')
        
        # add column ordering
        column_ordering = db_utils.build_column_ordering(im, self.ordering)
        if column_ordering is not None and len(column_ordering) > 0:
            for ordering_item in column_ordering:
                q = q.order_by(ordering_item)

        # limit number of returned records
        if self.record_limit is not None and self.record_limit > 0:
            q = q.limit(self.record_limit)

        instrument_metas = q.all()

        results = DataFrame()
        error_msg = None
        record_count = 0
        try:
            if len(instrument_metas) > 0:
                results = DataFrame(instrument_metas, columns = instrument_metas[0]._fields)
            
        except Exception as err:
            message = 'Request for instrument meta records FAILED'
            error_msg = f'Failed to get instrument meta records - err: {err}'
        else:
            message = 'Request for instrument meta records SUCCEEDED'
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