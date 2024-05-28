"""
Copyright 2023 NOAA
All rights reserved.

Collection of methods to facilitate handling of score db requests

"""

from collections import namedtuple
import copy
from dataclasses import dataclass, field
from datetime import datetime
import json
import pprint
from db_action_response import DbActionResponse
import score_table_models as stm
from score_table_models import StorageLocation as sl
import time_utils
import db_utils

from pandas import DataFrame
import sqlalchemy as db
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect
from sqlalchemy import and_, or_, not_
from sqlalchemy import asc, desc
from sqlalchemy.sql import func

StorageLocationData = namedtuple(
    'StorageLocationData',
    [
        'name',
        'bucket_name',
        'platform',
        'platform_region',
        'key'
    ]
)

@dataclass
class StorageLocation:
    ''' storage location object storing data related to file storage '''
    name: str
    bucket_name: str
    platform: str
    platform_region: str
    key: str 
    storage_location_data: StorageLocationData = field(init=False)

    def __post_init__(self):
        print(f'in post init name: {self.name}')
        self.storage_location_data = StorageLocationData(
            self.name,
            self.bucket_name,
            self.platform,
            self.platform_region,
            self.key
        )
    
    def __repr__(self):
        return f'storage_location_data: {self.storage_location_data}'

    def get_storage_location_data(self):
        return self.storage_location_data
    
def get_storage_location_from_body(body):
    if not isinstance(body, dict):
        msg = 'The \'body\' key must be a type dict, was ' \
            f'{type(body)}'
        raise TypeError(msg)
    
    storage_location = StorageLocation(
        body.get('name'),
        body.get('bucket_name'),
        body.get('platform'),
        body.get('platform_region'),
        body.get('key')
    )

    return storage_location

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

def construct_filters(filters):
    constructed_filter = {}

    constructed_filter = get_string_filter(
        filters, sl, 'name', constructed_filter)

    constructed_filter = get_string_filter(
        filters, sl, 'bucket_name', constructed_filter)

    constructed_filter = get_string_filter(
        filters, sl, 'platform', constructed_filter)

    constructed_filter = get_string_filter(
        filters, sl, 'platform_region', constructed_filter)
    
    constructed_filter = get_string_filter(
        filters, sl, 'key', constructed_filter)
    
    return constructed_filter

def get_all_storage_locations():
    request_dict = {
        'name': 'storage_location',
        'method': db_utils.HTTP_GET
    }
    
    slr = StorageLocationRequest(request_dict)
    return slr.submit()

@dataclass
class StorageLocationRequest:
    request_dict: dict
    method: str = field(default_factory=str, init=False)
    params: dict = field(default_factory=dict, init=False)
    filters: dict = field(default_factory=dict, init=False)
    ordering: list = field(default_factory=list, init=False)
    record_limit: int = field(default_factory=int, init=False)
    body: dict = field(default_factory=dict, init=False)
    storage_location: StorageLocation = field(init=False)
    storage_location_data: namedtuple = field(init=False)
    response: dict = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.method = db_utils.validate_method(self.request_dict.get('method'))
        self.params = self.request_dict.get('params')

        self.body = self.request_dict.get('body')
        if self.method == db_utils.HTTP_PUT:
            self.storage_location = get_storage_location_from_body(self.body)
            self.storage_location_data = self.storage_location.get_storage_location_data()
            for k, v in zip(
                self.storage_location_data._fields, self.storage_location_data
            ):
                val = pprint.pformat(v, indent=4)
                print(f'exp_data: k: {k}, v: {val}')
        else:
            print(f'In StorageLocationRequest - params: {self.params}')
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
            message='Failed storage location request.',
            details=None,
            errors=error_msg
        )
    
    def submit(self):
        if self.method == db_utils.HTTP_GET:
            return self.get_storage_locations()
        elif self.method == db_utils.HTTP_PUT:
            # becomes an update if record exists
            try:
                return self.put_storage_location()
            except Exception as err:
                error_msg = 'Failed to insert storage location record -' \
                    f' err: {err}'
                print(f'Submit PUT error: {error_msg}')
                return self.failed_request(error_msg)
            
    def put_storage_location(self):
        session = stm.get_session()

        insert_stmt = insert(sl).values(
            name=self.storage_location_data.name,
            bucket_name=self.storage_location_data.bucket_name,
            platform=self.storage_location_data.platform,
            platform_region=self.storage_location_data.platform_region,
            key=self.storage_location_data.key,
            created_at=datetime.utcnow(),
            updated_at=None
        ).returning(sl)
        print(f'insert_stmt: {insert_stmt}')

        time_now = datetime.utcnow()

        do_update_stmt = insert_stmt.on_conflict_do_update(
            constraint='unique_storage_location',
            set_=dict(
                bucket_name=self.storage_location_data.bucket_name,
                platform=self.storage_location_data.platform,
                platform_region=self.storage_location_data.platform_region,
                key=self.storage_location_data.key,
                updated_at=time_now
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
            message = f'Attempt to {action} storage location record FAILED'
            error_msg = f'Failed to insert/update record - err: {err}'
            print(f'error_msg: {error_msg}')
        else:
            message = f'Attempt to {action} storage location record SUCCEEDED'
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
    
    def get_storage_locations(self):
        session = stm.get_session()

        q = session.query(
            sl.id,
            sl.name,
            sl.bucket_name,
            sl.platform,
            sl.platform_region,
            sl.key,
            sl.created_at,
            sl.updated_at
        ).select_from(
            sl
        )

        print('Before adding filters to storage locations request########################')
        if self.filters is not None and len(self.filters) > 0:
            for key, value in self.filters.items():
                q = q.filter(value)
        
        print('After adding filters to storage location request########################')
        
        # add column ordering
        column_ordering = db_utils.build_column_ordering(sl, self.ordering)
        if column_ordering is not None and len(column_ordering) > 0:
            for ordering_item in column_ordering:
                q = q.order_by(ordering_item)

        # limit number of returned records
        if self.record_limit is not None and self.record_limit > 0:
            q = q.limit(self.record_limit)

        storage_locations = q.all()

        results = DataFrame()
        error_msg = None
        record_count = 0
        try:
            if len(storage_locations) > 0:
                results = DataFrame(storage_locations, columns = storage_locations[0]._fields)
            
        except Exception as err:
            message = 'Request for storage location records FAILED'
            error_msg = f'Failed to get storage location records - err: {err}'
        else:
            message = 'Request for storage location records SUCCEEDED'
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