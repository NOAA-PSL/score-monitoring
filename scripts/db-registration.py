import sys
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import db_yaml_generator 
import os
import pathlib
import datetime as dt
from dotenv import load_dotenv
import subprocess
import json


def register_experiment(experiment_configuration):
    cycle_start = "2016-01-01 00:00:00"
    cycle_end = "2016-01-31 00:00:00"
    owner_id = "score-monitoring.generated"
    group_id = "gsienkf"
    experiment_type = "EXAMPLE_REPLAY"
    platform = "aws"
    description = json.dump({"experiment configuration": experiment_configuration})

    yaml_file = db_yaml_generator.generate_exp_reg_yaml(cycle_start, cycle_end, owner_id, group_id, experiment_type, platform, description)
    subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file])


def register_storage_location():
    name = "replay_bucket"
    platform = "aws_s3"
    platform_region = "n/a"

    yaml_file = db_yaml_generator.generate_storage_loc_reg_yaml(name, platform, platform_region)
    subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file])


def register_file_type():
    name = "all_files_example"
    file_template = "*file.example"
    file_format = "text"
    description = "example for file type registration"

    yaml_file = db_yaml_generator.generate_file_type_reg_yaml(name, file_template, file_format, description)
    subprocess.run(["python3", os.getenv("SCORE_DB_BASE_LOCATION"), yaml_file])


def main():
    register_experiment()
    register_storage_location()
    register_file_type()

if __name__ == "__main__":
    main()
    