"""
Copyright 2022 NOAA
All rights reserved.

Collection request handlers registrations.  This module helps define
the request handler format as well as the module definitions for each
request type

"""
from collections import namedtuple
from regions import RegionRequest
from experiments import ExperimentRequest
from db_action_response import DbActionResponse
from expt_metrics import ExptMetricRequest
from metric_types import MetricTypeRequest
from harvest_innov_stats import HarvestInnovStatsRequest
from plot_innov_stats import PlotInnovStatsRequest
from file_types import FileTypeRequest
from storage_locations import StorageLocationRequest
from expt_file_counts import ExptFileCountRequest
from harvest_metrics import HarvestMetricsRequest
from array_metric_types import ArrayMetricTypeRequest
from sat_meta import SatMetaRequest
from expt_array_metrics import ExptArrayMetricRequest
from instrument_meta import InstrumentMetaRequest

NAMED_TUPLES_LIST = 'tuples_list'
PANDAS_DATAFRAME = 'pandas_dataframe'

INNOV_TEMPERATURE_NETCDF = 'innov_temperature_netcdf'


RequestHandler = namedtuple(
    'RequestHandler',
    [
        'description',
        'request',
        'result'
    ],
)

request_registry = {
    'region': RequestHandler(
        'Add or get regions',
        RegionRequest,
        DbActionResponse
    ),
    'experiment': RequestHandler(
        'Add or get or update experiment registration data',
        ExperimentRequest,
        DbActionResponse
    ),
    'expt_metrics': RequestHandler(
        'Add or get experiment metrics data',
        ExptMetricRequest,
        DbActionResponse
    ),
    'metric_types': RequestHandler(
        'Add or get or update metric types',
        MetricTypeRequest,
        DbActionResponse
    ),
    'harvest_innov_stats': RequestHandler(
        'Gather and store innovation statistics from diagnostics files',
        HarvestInnovStatsRequest,
        DbActionResponse
    ),
    'plot_innov_stats': RequestHandler(
        'Plot innovation statistics',
        PlotInnovStatsRequest,
        DbActionResponse
    ),
    'file_types': RequestHandler(
        'Add or get or update file types',
        FileTypeRequest,
        DbActionResponse
    ),
    'storage_locations': RequestHandler(
        'Add or get or update storage locations',
        StorageLocationRequest,
        DbActionResponse
    ),
    'expt_file_counts': RequestHandler(
        'Add or get experiment file counts',
        ExptFileCountRequest,
        DbActionResponse
    ),
    'harvest_metrics': RequestHandler(
        'Harvest and store metrics',
        HarvestMetricsRequest,
        DbActionResponse
    ),
    'array_metric_types': RequestHandler(
        'Add or get or update array metric types',
        ArrayMetricTypeRequest,
        DbActionResponse
    ),
    'sat_meta': RequestHandler(
        'Add or get or update sat metadata',
        SatMetaRequest,
        DbActionResponse
    ),
    'expt_array_metrics': RequestHandler(
        'Add or get experiment array metrics data',
        ExptArrayMetricRequest,
        DbActionResponse
    ),
    'instrument_meta': RequestHandler(
        'Add or get or update instrument meta data',
        InstrumentMetaRequest,
        DbActionResponse
    )
}
