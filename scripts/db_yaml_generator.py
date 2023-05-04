import yaml 
import pathlib
import os
import datetime as dt
from dotenv import load_dotenv
import json

PY_CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

load_dotenv()

YAML_FILE_PREFIX = 'monitoring-yaml-'

def generate_exp_reg_yaml(cycle_start, cycle_end, owner_id, group_id, experiment_type, platform, description):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, YAML_FILE_PREFIX + dt.datetime.now().strftime("%Y%m%d%H%M%S") + '.yaml')
    
    body = {
        'db_request_name' : 'experiment',
        'method': 'PUT',
        'body' : {
            'name' : os.getenv('EXPERIMENT_NAME'),
            'datestr_format' : "%Y-%m-%d %H:%M:%S",
            'cycle_start' : cycle_start,
            'cycle_stop' : cycle_end,
            'owner_id' : owner_id,
            'group_id' : group_id,
            'experiment_type' : experiment_type,
            'platform' : platform,
            'wallclock_start' : os.getenv('EXPERIMENT_WALLCLOCK_START'),
            'description' : description
        }
    }
    
    with open(yaml_file_path, 'w') as outfile:
        yaml.dump(body, outfile)
    return yaml_file_path

def generate_storage_loc_reg_yaml(name, platform, platform_region):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, YAML_FILE_PREFIX + dt.datetime.now().strftime("%Y%m%d%H%M%S") + '.yaml')

    body = {
        'db_request_name' : 'storage_locations',
        'method': 'PUT',
        'body' : {
            'name': name,
            'platform': platform,
            'bucket_name': os.getenv('STORAGE_LOCATION_BUCKET'),
            'key': os.getenv('STORAGE_LOCATION_KEY'),
            'platform_region': platform_region 
        }
    }

    with open(yaml_file_path, 'w') as outfile:
        yaml.dump(body, outfile)
    return yaml_file_path

def generate_file_type_reg_yaml(name, file_template, file_format, description):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, YAML_FILE_PREFIX + dt.datetime.now().strftime("%Y%m%d%H%M%S") + '.yaml')

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

def generate_metrics_yaml(name, region, elevation, elevation_unit, value, time_valid):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, YAML_FILE_PREFIX + dt.datetime.now().strftime("%Y%m%d%H%M%S") + '.yaml')
    
    body = {
        'db_request_name' : 'expt_metrics',
        'method': 'PUT',
        'body' : {
            'expt_name': os.getenv('EXPERIMENT_NAME'),
            'expt_wallclock_start': os.getenv('EXPERIMENT_WALLCLOCK_START'),
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

def generate_file_count_yaml(count, file_type, time_valid, forecast_length, folder_path, cycle):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, YAML_FILE_PREFIX + dt.datetime.now().strftime("%Y%m%d%H%M%S") + '.yaml')

    body = {
        'db_request_name': 'expt_file_counts',
        'method': 'PUT',
        'body': {
            'experiment_name': os.getenv('EXPERIMENT_NAME'),
            'wallclock_start': os.getenv('EXPERIMENT_WALLCLOCK_START'),
            'file_type_name': file_type,
            'time_valid': time_valid,
            'forecast_length' : forecast_length,
            'bucket_name' : os.getenv('STORAGE_LOCATION_BUCKET'),
            'platform': os.getenv('STORAGE_LOCATION_PLATFORM'),
            'key': os.getenv('STORAGE_LOCATION_KEY'),
            'count': count,
            'folder_path': folder_path,
            'cycle': cycle
        }
    }

    with open(yaml_file_path, 'w') as outfile:
        yaml.dump(body, outfile)
    return yaml_file_path

