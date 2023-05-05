"""
Copyright 2023 NOAA
All rights reserved.

Script for inital registration of values in score-db necessary for other scripts to save cycle specific values.
Should be customized with each variable per function as necessary and it may be that the main function should be changed to only call
whichever registration is needed at the time.
Functions need to only be called once per variable combination. 
"""

import db_yaml_generator 
import os
from dotenv import load_dotenv
import subprocess
import json

#registers an experiment, datetimes are expected in format: "%Y-%m-%d %H:%M:%S"
def register_experiment(experiment_configuration):
    cycle_start = "2016-01-01 00:00:00"
    cycle_end = "2016-01-31 00:00:00"
    owner_id = "score-monitoring.generated"
    group_id = "gsienkf"
    experiment_type = "EXAMPLE_REPLAY"
    platform = "pw_awv2"
    description = json.dumps({"experiment configuration": experiment_configuration})

    yaml_file = db_yaml_generator.generate_exp_reg_yaml(cycle_start, cycle_end, owner_id, group_id, experiment_type, platform, description)
    subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file])
    os.remove(yaml_file)

#register the storage location, utilizes environment variables
def register_storage_location():
    name = "replay_bucket"
    platform = "aws_s3"
    platform_region = "n/a"

    yaml_file = db_yaml_generator.generate_storage_loc_reg_yaml(name, platform, platform_region)
    subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file])
    os.remove(yaml_file)

#register the file type 
def register_file_type():
    name = "all_files_example"
    file_template = "*file.example"
    file_format = "text"
    description = "example for file type registration"

    yaml_file = db_yaml_generator.generate_file_type_reg_yaml(name, file_template, file_format, description)
    subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file])
    os.remove(yaml_file)


def main():
    load_dotenv()
    register_experiment("input experiment configuration description here")
    register_storage_location()
    register_file_type()

if __name__ == "__main__":
    main()
