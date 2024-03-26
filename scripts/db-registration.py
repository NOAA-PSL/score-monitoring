"""
Copyright 2023 NOAA
All rights reserved.

Script for inital registration of values in score-db necessary for other scripts to save cycle specific values.
The user should customize each variable per function as necessary and it may be that the main function should be changed to only call
whichever registration is needed at the time for that particular use case. 
Functions need to only be called once per variable combination. 

Required input for main argument when running script of environment file name to use (such as ../.env-example)
Example call: python3 db-registration.py ../.env-example 
"""
import sys
import db_yaml_generator 
import os
from dotenv import load_dotenv
import subprocess
import json
import argparse

#registers an experiment, datetimes are expected in format: "%Y-%m-%d %H:%M:%S"
def register_experiment(experiment_configuration):
    #USER DEFINED VARIABLES
    cycle_start = "1994-01-01 00:00:00"
    cycle_end = "2019-01-01 00:00:00"
    owner_id = "score-monitoring.generated"
    group_id = "gsienkf"
    experiment_type = "scout_runs"
    platform = "pw_awv2"
    description = json.dumps({"experiment configuration": experiment_configuration})
    #END USER DEFINED VARIABLES

    name = os.getenv('EXPERIMENT_NAME')
    print(f'begin registering experiment: {name}')
    yaml_file = db_yaml_generator.generate_exp_reg_yaml(name, os.getenv('EXPERIMENT_WALLCLOCK_START'), cycle_start, 
                                                        cycle_end, owner_id, group_id, experiment_type, platform, description)
    subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file])
    os.remove(yaml_file)
    print(f'end registering experiment')

#register the storage location, utilizes environment variables
def register_storage_location():
    #USER DEFINED VARIABLES
    name = "replay_bucket"
    platform_region = "n/a"
    #END USER DEFINED VARIABLES

    print(f'begin registering storage location: {name}')
    yaml_file = db_yaml_generator.generate_storage_loc_reg_yaml(name, os.getenv('STORAGE_LOCATION_BUCKET'), os.getenv('STORAGE_LOCATION_KEY'), 
                                                                os.getenv('STORAGE_LOCATION_PLATFORM'), platform_region)
    subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file])
    os.remove(yaml_file)
    print(f'end registering storage location')

#register the file type 
def register_file_type():
    #USER DEFINED VARIABLES
    name = "all_files_example"
    file_template = "*file.example"
    file_format = "text"
    description = "example for file type registration"
    #END USER DEFFINED VARIABLES 
    
    print(f'begin registering file type: {name}')
    yaml_file = db_yaml_generator.generate_file_type_reg_yaml(name, file_template, file_format, description)
    subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file])
    os.remove(yaml_file)
    print(f'end registering file type')

#register the metric type
def register_metric_type():
    #USER DEFINED VARIABLES
    name = "metric_name"
    long_name = "Long name of metric"
    measurement_type = "measurement_type"
    units = "measurement_units"
    stat_type = "stat_type"
    description = json.dumps({"type_description": "description information"})
    #END USER DEFINED VARIABLES

    print(f'begin registering metric type: {name}')
    yaml_file = db_yaml_generator.generate_metric_type_reg_yaml(name, long_name, measurement_type, units, stat_type, description)
    subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file])
    os.remove(yaml_file)
    print(f'end registering metric type')

def main():
    #set up arg parser to provide --help and -h flags and check for required argument
    parser = argparse.ArgumentParser(description="Required input for main argument when running script is path to environment file name to use (such as ../.env-example)" +
                    "Example call: python3 db-registration.py ../.env-example ")
    parser.add_argument('input_env', help="file name and relative location of the environment file")
    args = parser.parse_args()

    #import env variables
    print(f"Input: {args.input_env}")
    assert os.path.isfile(args.input_env), f"File {args.input_env} was not found, please provide the path to .env* file"
    load_dotenv(args.input_env)
    print(f"{args.input_env} environment loaded.")

    #USER SHOULD COMMENT / UNCOMMENT CALLS AS APPROPRIATE
    register_experiment("scout runs (GSI3DVar) replay observer diagnostic")
    register_storage_location()
    #register_file_type()
    #register_metric_type()

if __name__ == "__main__":
    main()
