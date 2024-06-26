# Score Monitoring Suite 

Initial development focused on monitoring integration for the replay experiments

Development on this repo is ongoing and more features will be added. Currently, it is able to register database metadata, store all file counts for an aws s3 bucket, and harvest and store metrics from increment logs from replay via cylc cycles. 

This repo contains a cylc suite and python scripts. It is dependent on having access to the score-db and score-hv repositories. The score-hv repo should be connected to the score-db repo via the bash script score_db_utils.sh in the score-db repo. Then the location of the score-db repo should be specified prior to running the score-monitoring scripts via the environment variable SCORE_DB_BASE_LOCATION in the user created .env file. 

# Structure

## Cylc Suite
For the cylc suite, users should specify the parameters in the suite.rc file prior to runs configured to their use case. The cylc suite will cycle through a given time period and run a file check in aws s3 (via python script) against the bucket specified in the .env in the parameters. If the file check fails, finds no files, or finds files are less than 30 minutes old, the cylc task will fail and retry in 1 hour.  After the file check is done, additional tasks will run such as storing file counts or metrics as specified in the suite graph and stats parameters.


## Python Scripts
Most of the python scripts are called via the cylc suite, except the db-registration.py script which must be run independently by the user as necessary. db-registration.py should be run on the first time using a specific variable in which the metadata must be first stored in the database, prior to running the cylc suite. Scripts called via the stats parameter in the cylc suite must be titled in the format of db_{stat}.py where stat is the same name that is listed under the stats parameter. 

The db-registration script will store the provided metadata into the database for experiments, storage locations, file types, and metric types. This metadata is requird in the database to store file counts and metrics. Each experiment, storage location, file type, and metric type will only need to be registered once. Usage of the script is dependent on the users specific use case and must be edited for the user defined input prior to each run. It is also possible the user may only want to register one type of metadata and in those cases the other function calls should be commented out in the main() function. For example, if you only need to add a new file type and metric type, you'd edit the file type metadata in the register_file_type() function and the metric type metadata in the register_metric_type() function and then comment out the register_experiment() and register_storage_location() functions from the main() function "#register_experiment() and #register_storage_location()".

# How To Run the Suite

## Setup

### **1. Install score-db and score-hv**
While some of the functionality of score-monitoring does not require database interactions, if using the db related code (most of the scripts and all of the data storage functionality), then the score-db and score-hv modules must be installed. 

To install, download the packages from [score-db](https://github.com/NOAA-PSL/score-db) and [score-hv](https://github.com/NOAA-PSL/score-hv). It is highly recommended to install the repositoritories into the same folder.

```
git clone https://github.com/NOAA-PSL/score-db
git clone https://github.com/NOAA-PSL/score-hv
```

After installing the suite-db repository, you will need to create a .env file based on the repository example and obtain the database password from the administrator. See the score-db [README](https://github.com/NOAA-PSL/score-db#installation-and-environment-setup) installiation step 6 for more details.

### **2. Create the .env file**

The user must define a '.env-*' file containing the appropriate information based on .env-example. The name of the file may be anything starting with .env-{user's choice}, this name will be passed into the code as a variable in the next step. This information will be referenced throughout the suite as needed. 

.env-* file format: 
```
EXPERIMENT_NAME = 'EXAMPLE-EXPERIMENT-NAME'
EXPERIMENT_WALLCLOCK_START = '2023-01-22 09:22:05'
STORAGE_LOCATION_BUCKET = 's3-bucket'
STORAGE_LOCATION_PLATFORM = 'aws_s3'
STORAGE_LOCATION_KEY = 'location/date_format/sub-directories'
SCORE_DB_BASE_LOCATION = '/path/to/score-db/src/score_db_base.py'
```

EXPERIMENT_NAME and EXPERIMENT_WALLCLOCK_START are user defined values which are used for registering experiments and then referencing that experiment when storing other data, including file counts and metrics. Once registered, these values need to stay consistent.
STORAGE_LOCATION_BUCKET is the root name of the S3 bucket, this must match what is in AWS.
STORAGE_LOCATION_PLATFORM is a metadata value used for referencing the storage location for registration and file counts.
STORAGE_LOCATION_KEY is the key location in the S3 bucket beneath the root to be used. This will be used for metadata registration and pulling data. The value can be an empty string if the top of the S3 bucket is being used and no '/' should be in the front of the key or at the end of the key. It should include any year, month, and date string replacement values using standard format codes (e.g., %Y/%m/%Y%m%d%H for the string formatted as [YYYY]/[MM]/[YYYYMMDDHH]). Documentation for Python's datetime format codes are provided at [https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes).
The SCORE_DB_BASE_LOCATION value will be the absolute path to score-db downloaded in step one to the specific level of the score_db_base.py script as show in the example. 

### **3. Update the cylc suite**
The cylc suite has four pieces of information that the user should define: email, cycle times, .env-* file name, and if desired stats to run data collection on. All of this will be edited within the *suite.rc* file.

Most of these values are found in the parameters section of the top of the *suite.rc* file. 

```
# parameters 
{% set MAIL_ADDRESS = 'first.last@noaa.gov' %}
{% set INITIAL_CYCLE_POINT = '20090501T06' %}
{% set FINAL_CYCLE_POINT = '20090503T18' %}
{% set ENV_PATH = '../.env-example' %}
```

Update each of these values to be appropriate for your user and experiment. The email address is used to notify the user of task failures. The ENV_PATH value should be relative to the locaion of the *suite.rc* file, likely starting with '../.env-*' if following the example pattern and location of .env-example. 

If desired, the user can change with stats will be stored based on the paramater 'stats' found under '[cylc]/[[parameters]]'. By default, this list is already updated with all current values available. stat values must be associated with a script of the form db_{stat}.py in the scripts folder to be valid. All values listed under stats will be stored via the GET_DATA step of the cylc graph.

```
[cylc]
    [[parameters]]
        stats = file_count, inc_logs
```

### **4. Register relevant information in the database**

If necessary, information may need to be pre-registered into the database for your cylc suite to store values correctly. Values which must be pre-regsitered in the database include metadata abbout experiments, storage locations, file types, and metric types. Each item must only be registered once and does not need to be re-registered for each run. 

To register values, update the appropriate function in *db-registration.py* and run the script using python3. Values which need to be updated by the user are flagged with a comment *#USER DEFINED VARIABLES* and closed with *#END USER DEFINED VARIABLES*.  

Experiment metadata will need to be registered once before storing any other data connected to the experiment. Experiment name and wallclock start are used as references and are pulled from the .env file for consistency.

Storage location metadata will need to registered when using a new bucket for the first time or a different key within a bucket for the first time, i.e. if changing any of the values referenced in the .env instead of the script.

File type metadata will need to be registered if a different file type is being used for the file count script.

Metric type metadata will need to be registered when harvesting new metrics, such as using the inc_logs script or when a new harvester functionality is added.

Depending on which values need to be registered, the user must first comment/uncomment function calls in the main() function as found below: 

```
    #USER SHOULD COMMENT / UNCOMMENT CALLS AS APPROPRIATE
    register_experiment("USER DEFINED INPUT FOR EXPERIMENT DESCRIPTION")
    register_storage_location()
    register_file_type()
    register_metric_type()
```

In order to run the script, the anaconda3 module must be active. If needed, please see the steps to load modules in step 6. 

Registration is run by calling the db-registration.py script with the .env file written in step 2 in place of the .env-example.

```
python3 db-registration.py ../.env-example
```

## Running the Suite 

### **5. Connect score-db and score-hv**

Once the score-db and score-hv packages are downloaded from step 1, score-db must be made aware of the location of score-hv. This can be done by running the bash script found in the top level of the score-db repository called score_db_utils.sh, if the repositories are in the same folder. Note, if running on the clusters, this call will also load the anaconda3 module. 

```
source score_db_utils.sh
```

If the score-db and score-hv repositories are not located in the same directory, you must manually connect score-db and score-hv using the following code: 

```
export SCORE_DB_HOME_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PYTHONPATH=$SCORE_DB_HOME_DIR/src
export PYTHONPATH=$PYTHONPATH:[absolute or relative path to score-hv]/src
```

### **6. Load the cylc and anaconda3 modules**
In order to run the suite, the cylc and anaconda3 modules must be active. The code is currently built on cylc version 7.9.3. 

It can be called from the [UFS-RNR-stack](https://github.com/HenryWinterbottom-NOAA/UFS-RNR-stack) modules to ensure all the necessary packages are included in the anaconda3 module. 

On ParallelWorks clusters:
```
module use -a /contrib/home/builder/UFS-RNR-stack/modules
module load anaconda3 cylc-flow
```

On Hera: 
```
module use -a /scratch2/BMC/gsienkf/UFS-RNR-stack/modules
module load anaconda3 cylc-flow
```

### **7. Register the suite** 
It is recommended to register the suite first for ease of monitoring but is not required to run the suite.

Within the folder containing the *suite.rc* file, call the cylc registration command with your desired name, in this case 'example-suite'. 

```
cylc register example-suite
```

If not calling the registration in the folder containing the suite.rc file, you must also specify the path to the folder. 

```
cylc register example-suite /path/to/suite
```

After you register your suite, you can confirm the registration by running the 'validate' command and confirming the suite is valid for 7.9.3. 

```
cylc validate example-suite
```

### **8. Run the suite**
Once the configurations are complete, the suite can be run using cylc commands. For the full list of commands to use, see the Cylc documentation.

```
cylc run example-suite
```

### **9. Monitor the suite**
While the suite is running, you have the option to monnitor the process using cylc's 'mon' command or by checking the files in the job output folders. 

```
cylc mon example-suite
```

### **10. Handling failures**
Some failures are expected in the design of the suite, particularly if files have not populated in the source storage location. 

 If the storage location does not contain any files or if any of the files are less than 30 minutes old, the FILE CHECK task will purposely fail and retry in 1 hour. This will continue 168 times to allow files to populate over time as necessary before the suite will completely fail. 

If one of the GET DATA tasks fails, they will also re-attempt the task in one hour for 168 times before failing the entire suite. If the calls to score-db fail, then the task will also fail. 

If a task fails, the job.out and job.err file outputs in the cylc work output folders will contain print out data that can be useful in diagnosing additional issues. You can read the logs under the cylc-run directory under the name of the example-suite.

On AWS cluster the cylc run directory is located on lustre:
```
cd /lustre/home/work/cylc-run/
```

For example, looking up the logs of the FILE_CHECK would be under:

```
cd /lustre/home/work/cylc-run/example-suite/log/job/CYCLE_TIME/FILE_CHECK/01
cat job.out
cat job.err
```
where CYCLE_TIME is the cycle you'd like to see such as 20050101T00.

### **11. Kill the suite**
If you need to stop the suite while it's running, you can call the cylc stop command. 

```
cylc stop example-suite
```

If you need the suite to stop immediately and stop any running tasks at the --now flags. One --now will stop after the task completes and two --now --now flags will interrupt the currently running task to stop immediately.

```
cylc stop --now --now example-suite
```
