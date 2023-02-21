import yaml 
import pathlib
import os
import datetime as dt

PY_CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

def generate_exp_reg_yaml(exp_name):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, 'replay-exp-reg-', dt.datetime.now().strftime("%Y%m%d%H%M%S"), '.yaml')
    
    body = {
        'db_request_name' : 'experiment',
        'method': 'PUT',
        'body' : {
            'name' : exp_name,
            'datestr_format' : "%Y-%m-%d %H:%M:%S",
            'cycle_start' : '',
            'cycle_stop' : '',
            'owner_id' : 'score-mointoring.generated',
            'group_id' : 'gsienkf',
            'experiment_type' : 'REPLAY',
            'platform' : 'aws',
            'wallclock_start' : '',
            'wallclock_end' : '',
            'description' : '{}'
        }
    }

    yaml_file = yaml.dump(body, yaml_file_path)
    return yaml_file_path

def generate_metrics_yaml(exp_name, metric_name):
    yaml_file_path = os.path.join(PY_CURRENT_DIR, 'replay-exp-reg-', dt.datetime.now().strftime("%Y%m%d%H%M%S"), '.yaml')
    
    body = {
        'db_request_name' : 'expt_metrics',
        'method': 'PUT',
        'body' : {
            'name' : exp_name,
            'datestr_format' : "%Y-%m-%d %H:%M:%S",
            'cycle_start' : '',
            'cycle_stop' : '',
            'owner_id' : 'score-mointoring.generated',
            'group_id' : 'gsienkf',
            'experiment_type' : 'REPLAY',
            'platform' : 'aws',
            'wallclock_start' : '',
            'wallclock_end' : '',
            'description' : '{}'
        }
    }

    yaml_file = yaml.dump(body, yaml_file_path)
    return yaml_file_path
