# Score Monitoring Suite 

Initial development focused on monitoring integration for the replay experiments

Development on this repo is ongoing and more features will be added. Currently, it is able to register database metadata and store all file counts for an aws s3 bucket via cylc cycles. 

This repo contains a cylc suite and python scripts. It is dependent on having access to the score-db and score-hv repositories. The score-hv repo should be connected to the score-db repo via the bash script score_db_utils.sh in the score-db repo. Then the location of the score-db repo should be specified prior to running the score-monitoring scripts via the environment variable SCORE_DB_BASE_LOCATION. 

# Code Structure

## Cylc Suite
For the cylc suite, users should specify the parameters in the suite.rc file prior to runs configured to their use case. The cylc suite will cycle through a given time period and run a file check in aws s3 (via python script) against the bucket specified in the .env in the parameters. If the file check fails, finds no files, or finds files are less than 30 minutes old, the cylc task will fail and retry in 1 hour.  After the file check is done, additional tasks will run such as storing file counts or metrics as specified in the suite graph and stats parameters.


## Python Scripts
Most of the python scripts are called via the cylc suite, except the db-registration.py script which must be run independently by the user as necessary.

The db-registration script will store the provided metadata into the database for experiments, storage locations, and file types. This metadata is requird in the database to store file counts and metrics. Each experiment, storage location, and file type will only need to be registered once. Usage of the script is dependent on the users specific use case and must be edited for the user defined inputd prior to each run. It is also possible the user may only want to register one type of metadata and in those cases the other function calls should be commented out in the main() function. For example, if you only need to add a new file type, you'd edit the file type metadata in the register_file_type() function and then comment out the register_experiment() and register_storage_location() functions from the main() function "#register_experiment() and #register_storage_location()".

# How To Run the Suite

### Setup

1. Install score-db and score-hv
While some of the functionality of this code does not require database interactions, if using the db related code (most of the scripts and all of the stat storage), then the score-db and score-hv modules must be installed. 

Download the packages from [score-db] (https://github.com/NOAA-PSL/score-db) and [score-hv](https://github.com/NOAA-PSL/score-hv). It is highly recommended to install the repositoritories into the same folder.

Once the score-db and score-hv packages are downloaded, score-db must be made aware of the location of score-hv. This can be done by running the bash script found in the top level of the score-db repository called score__db_utils.sh, if the repositories are in the same folder. Note, if running on the clusters, this call will also load the anaconda3 module. 

```
source score_db_utils.sh
```

If the score-db and score-hv modules are not located in the same file, you must manually connect score-db and score-hv using the following code: 

```
export SCORE_DB_HOME_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PYTHONPATH=$SCORE_DB_HOME_DIR/src
export PYTHONPATH=$PYTHONPATH:[absolute or relative path to score-hv]/src
```

2. Create the .env file

The user must define a '.env-*' file containing the appropriate information based on .env-example. The name of the file may be anything starting with .env-{user's choice}, this name will be passed into the code as a variable in the next step. This information will be referenced throughout the suite as needed. 

.env file format: 
```
EXPERIMENT_NAME = 'EXAMPLE-EXPERIMENT-NAME'
EXPERIMENT_WALLCLOCK_START = '2023-01-22 09:22:05'
STORAGE_LOCATION_BUCKET = 's3-bucket'
STORAGE_LOCATION_PLATFORM = 'aws_s3'
STORAGE_LOCATION_KEY = 'location/key'
SCORE_DB_BASE_LOCATION = '/path/to/score-db/src/score_db_base.py'
```

The SCORE_DB_BASE_LOCATION value will be the absolute path to score-db loaded in step one to the specific level of the score_db_base.py script as showin the example. 


3. Update the cylc suite
The cylc suite has four pieces of information that the user should define: email, cycle times, .env file name, and if desired stats to run data collection on. All of this will be edited within the *suite.rc* file.

Most of these values are found in the parameters section of the top of the suite.rc file. 

```
# parameters 
{% set MAIL_ADDRESS = 'jessica.knezha@noaa.gov' %}
{% set INITIAL_CYCLE_POINT = '20090501T06' %}
{% set FINAL_CYCLE_POINT = '20090503T18' %}
{% set ENV_PATH = '../.env-example' %}
```

Update each of these values to be appropriate for your user and experiment. The email address is used to notify the user of task failures. The ENV_PATH value should be relative to the locaion of the suite.rc file, likely starting with '../.env' if following the example pattern. 

If desired, the user can change with stats will be stored bbased on the paramater 'stats' found under '[cylc]'. By default, this list is already updated with all current values available. stat values must be associated with a script of the form db_{stat}.py in the scripts folder to be valid. 

```
[cylc]
    UTC mode = True
    cycle point format = %Y%m%dT%H
    [[environment]]
        MAIL_ADDRESS = {{ MAIL_ADDRESS }}
    [[parameters]]
        **stats** = file_count, inc_logs
```

4. Register relevant information in the database

If necessary, information may need to be pre-registered into the database for your cylc suite to store values correctly. Values which must be pre-regsitered in the database include experiments, storage locations, file types, and metric types. Each item must only be registered once and does not need to be re-registered for each run. 

To register values, update the appropriate function in *db-registration.py* and run the script using python3. Values which need to be updated by the user are flagged with a comment *#USER DEFINED VARIABLES* and closed with *#END USER DEFINED VARIABLES*. Details about these specific values can be found in the Appendix. 

Depending on which values need to be registered, the user must first comment/uncomment function calls in the main() function as found below: 

```
    #USER SHOULD COMMENT / UNCOMMENT CALLS AS APPROPRIATE
    register_experiment("USER DEFINED INPUT FOR EXPERIMENT DESCRIPTION")
    register_storage_location()
    register_file_type()
    register_metric_type()
```
Comments are added by putting a *#* (hashtag) in front of the line of code. Comment out any functions which are *not* being used for registration prior to running the script. 

In order to run the script, the anaconda3 module must be active. If the bash script in step 1 was utililzed, this is already active. If not, please see the instructions on loading the modules in step 4 below, then return and run the registration script. 

Registration is run by calling the db-registration.py script with the .env file written in step 2 in place of the .env-example.

```
python3 db-registration.py ../.env-example
```

### Running the Suite 

4. Load the cylc and anaconda3 modules
In order to run the suite, the cylc and anaconda3 modules must be active. We are using cylc version 7.9.3. 

It can be called from the UFS-RNR-stack modules to ensure all the necessary package are also included in the anaconda3 module. 

On clusters:
```
module use -a /contrib/home/builder/UFS-RNR-stack/modules
module load anaconda3 cylc-flow
```

On Hera: 
```
module use -a /scratch2/BMC/gsienkf/UFS-RNR-stack/modules
module load anaconda3 cylc-flow
```

5. Register the suite 
It is recommended to register the suite first for ease of monitoring but is not required to run the suite.

Within the folder containing the *suite.rc* file, call the cylc registration command with your desired name, in this case 'example-suite'. 

```
cylc register example-suite
```

If not calling the registration in the folder containing the suite.rc file, you must also specify the path to the folder. 

```
cylc register example-suite /path/to/suite
```

6. Run the suite
# FLESH OUT THIS PART

```
cylc run example-suite
```

7. Monitor the suite
# FLESH OUT THIS PART
```
cylc mon example-suite
```

8. Handling failures


