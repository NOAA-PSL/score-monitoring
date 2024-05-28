"""
Copyright 2022 NOAA
All rights reserved.

Collection of methods to facilitate handling of score db requests

"""
from collections import namedtuple
import copy
from dataclasses import dataclass, field
from datetime import datetime
import json
import matplotlib.pyplot as plt
import matplotlib.pylab as pl
import numpy as np
import os
import pathlib

import pprint
import traceback

import pandas as pd
from pandas import DataFrame

import time_utils
from time_utils import DateRange
from score_hv.harvester_base import harvest
from expt_metrics import ExptMetricInputData, ExptMetricRequest
from innov_stats_plot_attrs import plot_attrs, region_labels


RequestData = namedtuple(
    'RequestData',
    [
        'datetime_str',
        'experiment',
        'metric_format_str',
        'metric',
        'stat',
        'regions',
        'elevation_unit',
        'time_valid'
    ],
)


@dataclass
class StatGroupData:
    stat_group_dict: dict
    cycles: list = field(default_factory=list, init=False)
    stat_group_frmt_str: str = field(default_factory=str, init=False)
    metrics: list[str] = field(init=False)
    stats: list[str] = field(init=False)
    regions: list[str] = field(init=False)
    elevation_unit: str = field(init=False)

    def __post_init__(self):
        self.cycles = self.stat_group_dict.get('cycles')
        self.stat_group_frmt_str = self.stat_group_dict.get('stat_group_frmt_str')
        self.metrics = self.stat_group_dict.get('metrics')
        self.stats = self.stat_group_dict.get('stats')
        self.regions = self.stat_group_dict.get('regions')
        self.elevation_unit = self.stat_group_dict.get('elevation_unit')


def get_experiment_metrics(request_data):
    
    expt_metric_name = request_data.metric_format_str.replace(
        '{metric}', request_data.metric
    )

    expt_metric_name = expt_metric_name.replace(
        '{stat}', request_data.stat
    )

    time_valid_from = datetime.strftime(
        request_data.time_valid.start, request_data.datetime_str)

    time_valid_to = datetime.strftime(
        request_data.time_valid.end, request_data.datetime_str)

    request_dict = {
        'name': 'expt_metrics',
        'method': 'GET',
        'params': {
            'datestr_format': request_data.datetime_str,
            'filters': {
                'experiment': request_data.experiment,    
                'metric_types': {
                    'name': {
                        'exact': [expt_metric_name]
                    },
                    'stat_type': {
                        'exact': [request_data.stat]
                    }
                },
                'regions': {
                    'name': {
                        'exact': request_data.regions
                    },
                },
                'time_valid': {
                    'from': time_valid_from,
                    'to': time_valid_to,
                },
                'elevation_unit': {
                    'exact': [request_data.elevation_unit]
                }
            },
            'ordering': [
                {'name': 'time_valid', 'order_by': 'asc'},
                {'name': 'elevation', 'order_by': 'desc'}
            ]
        }
    }

    print(f'request_dict: {request_dict}')

    emr = ExptMetricRequest(request_dict)
    result = emr.submit()

    return result.details['records']


def build_base_figure():
    fig = plt.figure()
    ax = plt.subplot()
    # ax.cla()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tick_params(
        axis='x',
        which='both',
        bottom=False,
        top=False,
        labelbottom=True
    )
    
    return (plt, fig, ax)


def format_figure(plt, ax, pa, region_label):

    plt.title(region_label)
    plt.gca().set_xlim([pa.axes_attrs.xmin, pa.axes_attrs.xmax])
    plt.gca().set_ylim([pa.axes_attrs.ymin, pa.axes_attrs.ymax])
    
    xticks = np.arange(
        pa.axes_attrs.xmin,
        (pa.axes_attrs.xmax + 1.e-6),
        pa.axes_attrs.xint
    )   
    plt.xticks(xticks)
    
    plt.xlabel(
        xlabel=pa.xlabel.label,
        horizontalalignment=pa.xlabel.horizontalalignment
    )
    
    plt.ylabel(
        ylabel=pa.ylabel.label,
        horizontalalignment=pa.ylabel.horizontalalignment
    )
    
    plt.gca().invert_yaxis()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    if pa.stat == 'bias':
        plt.vlines(
            x=0.0,
            ymin=pa.axes_attrs.ymin,
            ymax=pa.axes_attrs.ymax,
            linestyles='dashed',
            colors='gray',
            linewidth=0.5
        )

    plt.legend(
        loc=pa.legend.loc,
        fancybox=pa.legend.fancybox,
        edgecolor=pa.legend.edgecolor,
        framealpha=pa.legend.framealpha,
        shadow=pa.legend.shadow,
        facecolor=pa.legend.facecolor,
    )


def build_fig_dest(
    work_dir,
    fig_base_fn,
    metric,
    stat,
    region,
    date_range
):
    
    start = datetime.strftime(date_range.start, '%Y%m%dT%HZ')
    end = datetime.strftime(date_range.end, '%Y%m%dT%HZ')
    dest_fn = fig_base_fn
    dest_fn += f'__{metric}_{stat}_{region}__{start}_to_{end}.png'
    
    dest_full_path = os.path.join(work_dir, dest_fn)
    
    parent_dir = pathlib.Path(dest_full_path).parent
    pathlib.Path(parent_dir).mkdir(parents=True, exist_ok=True)
    return dest_full_path


def save_figure(plt, dest_full_path):
    print(f'saving figure to {dest_full_path}')
    plt.savefig(dest_full_path)


def plot_innov_stats(
    experiments,
    metric,
    stat,
    metrics_df,
    work_dir,
    fig_base_fn,
    date_range
):

    if not isinstance(metrics_df, DataFrame):
        msg = 'Input data to plot_innov_stats must be type pandas.DataFrame '\
            f'was actually type: {type(metrics_df)}'
        raise TypeError(msg)
    
    plt_attr_key = f'{metric}_{stat}'
    pa = plot_attrs[plt_attr_key]
    
    ave_df = metrics_df.groupby(
        ['expt_name', 'elevation', 'region'], as_index=False
    )['value'].mean()
        
    # loop through regions
    regions = ave_df.drop_duplicates(
        ['region'], keep='last'
    )['region'].values.tolist()
    
    expt_names = ave_df.drop_duplicates(
        ['expt_name'], keep='last'
    )['expt_name'].values.tolist()

    for region in regions:
        # if region != 'global':
        #     continue

        (plt, fig, ax) = build_base_figure()

        for expt in experiments:
            
            expt_name = expt.get('expt_name')
            stat_vals = ave_df.loc[
                (ave_df['region'] == region) &
                (ave_df['expt_name'] == expt_name),
                'value'
            ]

            elevations = ave_df.loc[
                (ave_df['region'] == region) &
                (ave_df['expt_name'] == expt_name),
                'elevation'
            ]

            plt.plot(
                stat_vals,
                elevations,
                color=expt.get('graph_color'),
                label=expt.get('graph_label')
            )
        
        format_figure(plt, ax, pa, region_labels[region])

        fig_fn = build_fig_dest(\
            work_dir,
            fig_base_fn,
            metric,
            stat,
            region,
            date_range
        )

        save_figure(plt, fig_fn)


@dataclass
class ExperimentData(object):
    name: str
    wallclock_start: str
    graph_color: str
    graph_label: str
    
    def get_dict(self):
        return {
            'name': {
                'exact': self.name
            },
            'wallclock_start': {
                'from': self.wallclock_start,
                'to': self.wallclock_start
            },
            'expt_name': self.name,
            'expt_start': self.wallclock_start,
            'graph_label': self.graph_label,
            'graph_color': self.graph_color
        }


@dataclass
class PlotInnovStatsRequest(object):
    config_dict: dict
    date_range: DateRange = field(init=False)
    stat_groups: list = field(default_factory=list, init=False)
    datetime_str: str = field(default_factory=str, init=False)
    experiments: list = field(default_factory=list, init=False)
    work_dir: str = field(default_factory=str, init=False)
    fig_base_fn: str = field(default_factory=str, init=False)

    def __post_init__(self):
        date_range_dict = self.config_dict.get('date_range')
        self.datetime_str = date_range_dict.get('datetime_str')
        start_str = date_range_dict.get('start')
        end_str = date_range_dict.get('end')
        
        self.experiments = self.get_experiments(self.config_dict)
        
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

        stat_groups = self.config_dict.get('stat_groups')
        if not isinstance(stat_groups, list):
            trcbk = traceback.format_exc()
            msg = 'No stat_groups key found or invalid type: ' \
                f'stat_groups: {stat_groups}, type(stat_groups): ' \
                f'{type(stat_groups)}, err: {trcbk}'
            raise TypeError(msg)

        for stat_group_dict in stat_groups:
            if not isinstance(stat_group_dict, dict):
                trcbk = traceback.format_exc()
                msg = 'stat_group_dict is invalid type: ' \
                    f'stat_group_dict: {stat_group_dict}, ' \
                    f'type(stat_group_dict): {type(stat_group_dict)}, ' \
                    f'err: {trcbk}'
                raise TypeError(msg)

            self.stat_groups.append(StatGroupData(stat_group_dict))
        
        self.work_dir = self.config_dict.get('work_dir')
        self.fig_base_fn = self.config_dict.get('fig_base_fn')


    def get_experiments(self, config_dict):
        
        experiments = config_dict.get('experiments')
        if not isinstance(experiments, list):
            msg = 'The \'experiments\' must be type list, actually ' \
                f'{type(experiments)}'
            raise TypeError(msg)

        experiments_data = []
        for experiment in experiments:
            if not isinstance(experiment, dict):
                msg = 'Each \'experiment\' must be type dict, actually ' \
                    f'{type(experiment)}'
                raise TypeError(msg)
        
            name = experiment.get('name')
            wallclk_strt = experiment.get('wallclock_start')
            graph_color = experiment.get('graph_color')
            graph_label = experiment.get('graph_label')
            expt_data = ExperimentData(
                name, wallclk_strt, graph_color, graph_label
            )
            experiments_data.append(expt_data.get_dict())

        return experiments_data


    def submit(self):
        
        master_list = []
        n_hours = 6
        n_days = 0

        finished = False
        loop_count = 0
        
        for stat_group in self.stat_groups:
            elevation_unit = stat_group.elevation_unit
            metrics_data = []
            # gather experiment metrics data for experiment and date range
            for metric in stat_group.metrics:
                for stat in stat_group.stats:
                    m_df = DataFrame()
                    for experiment in self.experiments:
                        request_data = RequestData(
                            self.datetime_str,
                            experiment,
                            stat_group.stat_group_frmt_str,
                            metric,
                            stat,
                            stat_group.regions,
                            stat_group.elevation_unit,
                            self.date_range
                        )

                        e_df = get_experiment_metrics(request_data)
                        e_df = e_df.sort_values(['expt_name', 'region', 'elevation'])
                        m_df = pd.concat([m_df, e_df], axis=0)

                    plot_innov_stats(
                        self.experiments,
                        metric,
                        stat,
                        m_df,
                        self.work_dir,
                        self.fig_base_fn,
                        self.date_range
                    )
