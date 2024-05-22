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
from score_table_models import MetricType as mt
import time_utils

from pandas import DataFrame
import sqlalchemy as db
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect
from sqlalchemy import and_, or_, not_
from sqlalchemy import asc, desc
from sqlalchemy.sql import func

ASCENDING = 'asc'
DESCENDING = 'desc'

VALID_ORDER_BY = [ASCENDING, DESCENDING]

HTTP_GET = 'GET'
HTTP_PUT = 'PUT'

VALID_METHODS = [HTTP_GET, HTTP_PUT]

INSERT = 'INSERT'
UPDATE = 'UPDATE'

EXACT_DATETIME = 'exact'
FROM_DATETIME = 'from'
TO_DATETIME = 'to'

HERA = 'hera'
ORION = 'orion'
PW_AZV1 = 'pw_azv1'
PW_AZV2 = 'pw_azv2'
PW_AWV1 = 'pw_awv1'
PW_AWV2 = 'pw_awv2'

VALID_PLATFORMS = [HERA, ORION, PW_AZV1, PW_AZV2, PW_AWV1, PW_AWV2]

def validate_column_name(cls, value):
    if not isinstance(value, str):
        raise TypeError(f'Column name must be a str, was {type(value)}')

    try:
        column_obj = getattr(cls, value)
        print(f'column: {column_obj}, type(key): {type(column_obj)}')
    except Exception as err:
        msg = f'Column does not exist - err: {err}'
        raise ValueError(msg)
    
    return column_obj


def validate_order_dir(value):
    if not isinstance(value, str):
        raise TypeError(f'\'order_by\' must be a str, was {type(value)}')
    
    if value not in VALID_ORDER_BY:
        raise TypeError(f'\'order_by\' must be one of {VALID_ORDER_BY}, was {value}')
    
    return value

def build_column_ordering(cls, ordering):
    """
    Build a sequential list of column ordering (otherwise known as the 
    ORDER BY clause)
    pertaining
    to the experiments table.

    Parameters:
    -----------
    cls: class object - this object can be any sqlalchemy table object
            such as Region, or Experiment
 
    ordering: list - this is a list of dicts which describe all the desired
        column data sequential ordering (or the ORDER BY sql clause).

        example list of orderby dicts: 
        [
            {'name': 'group_id', 'order_by': 'desc'},
            {'name': 'created_at', 'order_by': 'desc'}
        ]

    """
    if ordering is None:
        return None
    
    if not isinstance(ordering, list):
        msg = f'\'order_by\' must be a list - was: {type(ordering)}'
        raise TypeError(msg)

    constructed_ordering = []
    for value in ordering:
        print(f'value: {value}')

        if type(value) != dict:
            msg = f'List items must be a type-dict - was {type(value)}'
            raise TypeError(msg)

        col_obj = validate_column_name(cls, value.get('name'))
        order_by = validate_order_dir(value.get('order_by'))

        if order_by == ASCENDING:
            constructed_ordering.append(asc(col_obj))
        else:
            constructed_ordering.append(desc(col_obj))
    
    print(f'constructed_ordering: {constructed_ordering}')
    return constructed_ordering

def validate_method(method):
    if method not in VALID_METHODS:
        msg = f'Request type must be one of: {VALID_METHODS}, actually: {method}'
        print(msg)
        raise ValueError(msg)
    
    return method