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
from score_table_models import FileType as ft
import time_utils
import db_utils

from pandas import DataFrame
import sqlalchemy as db
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect
from sqlalchemy import and_, or_, not_
from sqlalchemy import asc, desc
from sqlalchemy.sql import func

FileTypeData = namedtuple(
    'FileTypeData',
    [
        'name',
        'file_template',
        'file_format',
        'description'
    ],
)

@dataclass
class FileType:
    ''' file type object storing data related to the file info '''
    name: str
    file_template: str
    file_format: str
    description: dict
    file_type_data: FileTypeData = field(init=False)

    def __post_init__(self):
        print(f'in post init name: {self.name}')
        print(f'description: {self.description}')
        self.file_type_data = FileTypeData(
            self.name,
            self.file_template,
            self.file_format,
            self.description
        )


    def __repr__(self):
        return f'file_type_data: {self.file_type_data}'


    def get_file_type_data(self):
        return self.file_type_data
    
def get_file_type_from_body(body):
    if not isinstance(body, dict):
        msg = 'The \'body\' key must be a type dict, was ' \
            f'{type(body)}'
        raise TypeError(msg)
    
    try:
        description = json.loads(body.get('description'))
    except Exception as err:
        msg = 'Error loading \'description\', must be valid JSON - err: {err}'
        raise ValueError(msg) from err

    file_type = FileType(
        body.get('name'),
        body.get('file_template'),
        body.get('file_format'),
        description
    )
    
    return file_type

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
        filters, ft, 'name', constructed_filter)

    constructed_filter = get_string_filter(
        filters, ft, 'file_template', constructed_filter)

    constructed_filter = get_string_filter(
        filters, ft, 'file_format', constructed_filter)
    
    return constructed_filter

def get_all_file_types():
    request_dict = {
        'name': 'file_type',
        'method': db_utils.HTTP_GET
    }

    ftr = FileTypeRequest(request_dict)
    return ftr.submit()

@dataclass
class FileTypeRequest:
    request_dict: dict
    method: str = field(default_factory=str, init=False)
    params: dict = field(default_factory=dict, init=False)
    filters: dict = field(default_factory=dict, init=False)
    ordering: list = field(default_factory=list, init=False)
    record_limit: int = field(default_factory=int, init=False)
    body: dict = field(default_factory=dict, init=False)
    file_type: FileType = field(init=False)
    file_type_data: namedtuple = field(init=False)
    response: dict = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.method = db_utils.validate_method(self.request_dict.get('method'))
        self.params = self.request_dict.get('params')

        self.body = self.request_dict.get('body')
        if self.method == db_utils.HTTP_PUT:
            self.file_type = get_file_type_from_body(self.body)
            self.file_type_data = self.file_type.get_file_type_data()
            for k, v in zip(
                self.file_type_data._fields, self.file_type_data
            ):
                val = pprint.pformat(v, indent=4)
                print(f'exp_data: k: {k}, v: {val}')
        else:
            print(f'In FileTypeRequest - params: {self.params}')
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
            message='Failed file type request.',
            details=None,
            errors=error_msg
        )


    def submit(self):
        if self.method == db_utils.HTTP_GET:
            return self.get_file_types()
        elif self.method == db_utils.HTTP_PUT:
            # becomes an update if record exists
            try:
                return self.put_file_type()
            except Exception as err:
                error_msg = 'Failed to insert file type record -' \
                    f' err: {err}'
                print(f'Submit PUT error: {error_msg}')
                return self.failed_request(error_msg)

    
    def put_file_type(self):
        session = stm.get_session()

        insert_stmt = insert(ft).values(
            name=self.file_type_data.name,
            file_template=self.file_type_data.file_template,
            file_format=self.file_type_data.file_format,
            description=self.file_type_data.description,
            created_at=datetime.utcnow(),
            updated_at=None
        ).returning(ft)
        print(f'insert_stmt: {insert_stmt}')

        time_now = datetime.utcnow()

        do_update_stmt = insert_stmt.on_conflict_do_update(
            constraint='unique_file_type',
            set_=dict(
                file_template=self.file_type_data.file_template,
                file_format=self.file_type_data.file_format,
                description=self.file_type_data.description,
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
            message = f'Attempt to {action} file type record FAILED'
            error_msg = f'Failed to insert/update record - err: {err}'
            print(f'error_msg: {error_msg}')
        else:
            message = f'Attempt to {action} file type record SUCCEEDED'
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

    
    def get_file_types(self):
        session = stm.get_session()

        q = session.query(
            ft.id,
            ft.name,
            ft.file_template,
            ft.file_format,
            ft.description,
            ft.created_at,
            ft.updated_at
        ).select_from(
            ft
        )

        print('Before adding filters to file types request########################')
        if self.filters is not None and len(self.filters) > 0:
            for key, value in self.filters.items():
                q = q.filter(value)
        
        print('After adding filters to file types request########################')
        
        # add column ordering
        column_ordering = db_utils.build_column_ordering(ft, self.ordering)
        if column_ordering is not None and len(column_ordering) > 0:
            for ordering_item in column_ordering:
                q = q.order_by(ordering_item)

        # limit number of returned records
        if self.record_limit is not None and self.record_limit > 0:
            q = q.limit(self.record_limit)

        file_types = q.all()

        results = DataFrame()
        error_msg = None
        record_count = 0
        try:
            if len(file_types) > 0:
                results = DataFrame(file_types, columns = file_types[0]._fields)
            
        except Exception as err:
            message = 'Request for file type records FAILED'
            error_msg = f'Failed to get file type  records - err: {err}'
        else:
            message = 'Request for file type records SUCCEEDED'
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