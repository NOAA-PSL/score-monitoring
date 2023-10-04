"""
Copyright 2023 NOAA
All rights reserved.

Helper functions to generate yaml file inputs for score-db calls from other scripts.
Dependent on environmnet variables necessary per yaml file.
"""
import yaml 
import pathlib
import os
import datetime as dt
import json

PY_CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

YAML_FILE_PREFIX = 'monitoring-yaml-'

def generate_exp_reg_yaml(experiment_name, experiment_wallclock, cycle_start, cycle_end, owner_id, group_id, experiment_type, platform, description):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, YAML_FILE_PREFIX + dt.datetime.now().strftime("%Y%m%d%H%M%S") + '-exp_reg.yaml')
    
    body = {
        'db_request_name' : 'experiment',
        'method': 'PUT',
        'body' : {
            'name' : experiment_name,
            'datestr_format' : "%Y-%m-%d %H:%M:%S",
            'cycle_start' : cycle_start,
            'cycle_stop' : cycle_end,
            'owner_id' : owner_id,
            'group_id' : group_id,
            'experiment_type' : experiment_type,
            'platform' : platform,
            'wallclock_start' : experiment_wallclock,
            'description' : description
        }
    }
    
    with open(yaml_file_path, 'w') as outfile:
        yaml.dump(body, outfile)
    return yaml_file_path

def generate_storage_loc_reg_yaml(name, bucket, key, platform, platform_region):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, YAML_FILE_PREFIX + dt.datetime.now().strftime("%Y%m%d%H%M%S") + '-storage_loc.yaml')

    body = {
        'db_request_name' : 'storage_locations',
        'method': 'PUT',
        'body' : {
            'name': name,
            'platform': platform,
            'bucket_name': bucket,
            'key': key,
            'platform_region': platform_region 
        }
    }

    with open(yaml_file_path, 'w') as outfile:
        yaml.dump(body, outfile)
    return yaml_file_path

def generate_file_type_reg_yaml(name, file_template, file_format, description):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, YAML_FILE_PREFIX + dt.datetime.now().strftime("%Y%m%d%H%M%S") + '-file_type.yaml')

    body = {
        'db_request_name' : 'file_types',
        'method': 'PUT',
        'body' : {
            'name': name,
            'file_template': file_template,
            'file_format': file_format,
            'description': json.dumps({"type_description": description})
        }
    }

    with open(yaml_file_path, 'w') as outfile:
        yaml.dump(body, outfile)
    return yaml_file_path

def generate_store_metrics_yaml(name, region, elevation, elevation_unit, value, time_valid, experiment_name, experiment_wallclock):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, YAML_FILE_PREFIX + dt.datetime.now().strftime("%Y%m%d%H%M%S") + '-store_metrics.yaml')
    
    body = {
        'db_request_name' : 'expt_metrics',
        'method': 'PUT',
        'body' : {
            'expt_name': experiment_name,
            'expt_wallclock_start': experiment_wallclock,
            'metrics': {
                'name': name,
                'region_name': region,
                'elevation': elevation,
                'elevation_unit':elevation_unit,
                'value': value,
                'time_valid': time_valid
            },
            'datestr_format': '%Y-%m-%d %H:%M:%S',
        }
    }

    with open(yaml_file_path, 'w') as outfile:
        yaml.dump(body, outfile)
    return yaml_file_path

def generate_file_count_yaml(count, file_type, time_valid, forecast_length, folder_path, cycle, expt_name, expt_wallclock, bucket, platform, key):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, YAML_FILE_PREFIX + dt.datetime.now().strftime("%Y%m%d%H%M%S") + '-file_count.yaml')

    body = {
        'db_request_name': 'expt_file_counts',
        'method': 'PUT',
        'body': {
            'experiment_name': expt_name,
            'wallclock_start': expt_wallclock,
            'file_type_name': file_type,
            'time_valid': time_valid,
            'forecast_length' : forecast_length,
            'bucket_name' : bucket,
            'platform': platform,
            'key': key,
            'count': count,
            'folder_path': folder_path,
            'cycle': cycle
        }
    }

    with open(yaml_file_path, 'w') as outfile:
        yaml.dump(body, outfile)
    return yaml_file_path

def generate_harvest_metrics_yaml(experiment_name, experiment_wallclock, hv_translator, harvest_config):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, YAML_FILE_PREFIX + dt.datetime.now().strftime("%Y%m%d%H%M%S") + '-harvest_metrics.yaml')
    
    body = {
        'db_request_name' : 'harvest_metrics',
        'body' : {
            'expt_name': experiment_name,
            'expt_wallclock_start': experiment_wallclock,
            'datestr_format': '%Y-%m-%d %H:%M:%S',
        },
        'hv_translator': hv_translator,
        'harvest_config': harvest_config
    }

    with open(yaml_file_path, 'w') as outfile:
        yaml.dump(body, outfile)
    return yaml_file_path
 
def generate_metric_type_reg_yaml(name, long_name, measurement_type, units, stat_type, description):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, YAML_FILE_PREFIX + dt.datetime.now().strftime("%Y%m%d%H%M%S") + '-metric_type.yaml')
    
    body = {
        'db_request_name' : 'metric_types',
        'method': 'PUT',
        'body' : {
            'name': name,
            'long_name': long_name,
            'measurement_type': measurement_type,
            'measurement_units': units,
            'stat_type': stat_type,
            'description': description
        }
    }

    with open(yaml_file_path, 'w') as outfile:
        yaml.dump(body, outfile)
    return yaml_file_path
