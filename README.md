# Score Monitoring Suite 

cylc based workflow software for monitoring reanalysis scout runs and 
experiments

Development on this repo is ongoing and more features will be added. Currently, 
it is able to register database metadata, store all file counts for an aws s3 
bucket, and harvest and store metrics from GSI analysis logs via cylc cycles. 

This repo contains a cylc workflow and python scripts. It is dependent on the 
score-db and score-hv repositories, which have installable python packages.

# Structure

## Cylc Workflow
For the cylc workflow, users should specify the parameters in a flow.cylc file 
prior to runs configured to their use case. The cylc workflow will cycle through a 
given time period and run a file check in aws s3 (via python script) against 
the bucket specified in the .env in the parameters. If the file check fails, 
finds no files, or finds files are less than 30 minutes old, the cylc task will 
fail and retry.  After the file check is done, additional tasks will run such 
as storing file counts or GSI statistics as specified in the suite graph and 
stats parameters.

**Additional Test Workflow:** As part of the validation process for the 
environment setup, the suite includes a lightweight demonstration that tests 
the correct importation and operation of numpy and scipy. This process runs 
corresponding test suites on these libraries, ensuring the environment is 
properly configured for scientific computing tasks. These tests are executed 
on the default platform (e.g., background) and on the "batch_partition",
which should be properly configured in the global.cylc configuration file.
For more information on configuration, refer to:
https://cylc.github.io/cylc-doc/stable/html/reference/config/global.html

For example:

test_numpy: Runs a numpy test suite to check the installation of numpy.
test_scipy: Runs a scipy test suite to check the installation of scipy.
Both tasks are executed as part of the overall Cylc workflow and are run with 
a time limit of 6 hours.

## Python Scripts
Most of the python scripts are called via the cylc suite, except the 
db-registration.py script which must be run independently by the user as 
necessary. db-registration.py should be run on the first time using a specific 
variable in which the metadata must be first stored in the database, prior to 
running the cylc suite. Scripts called via the stats parameter in the cylc 
suite must be titled in the format of db_{stat}.py where stat is the same name 
that is listed under the stats parameter. 

The db-registration script will store the provided metadata into the database 
for experiments, storage locations, file types, and metric types. This metadata 
is requird in the database to store file counts and metrics. Each experiment, 
storage location, file type, and metric type will only need to be registered 
once. Usage of the script is dependent on the users specific use case and must 
be edited for the user defined input prior to each run. It is also possible the 
user may only want to register one type of metadata and in those cases the 
other function calls should be commented out in the main() function. For 
example, if you only need to add a new file type and metric type, you'd edit 
the file type metadata in the register_file_type() function and the metric type 
metadata in the register_metric_type() function and then comment out the 
register_experiment() and register_storage_location() functions from the main() 
function "#register_experiment() and #register_storage_location()".

# How To Run a Workflow

## Setup

### **1. Install score-db and score-hv**
While some of the functionality of score-monitoring does not require database 
interactions, if using the db related code (most of the scripts and all of the 
data storage functionality), then the score-db and score-hv modules must be 
installed. Before installing the suite-db package, you will need to create a 
.env file in score-db based on the repository example and obtain the database 
password from the administrator. Note that this .env file should be placed with 
the package source code (src/score_db/.env) for pip installs. See the score-db 
[README](https://github.com/NOAA-PSL/score-db?tab=readme-ov-file#installation-and-environment-setup) installation step 5 for more details.

To install, download the packages from 
[score-db](https://github.com/NOAA-PSL/score-db) and 
[score-hv](https://github.com/NOAA-PSL/score-hv). It is highly recommended to 
install the repositoritories into a conda environment.

```
git clone https://github.com/NOAA-PSL/score-db
cd score-db
pip install .
```

```
git clone https://github.com/NOAA-PSL/score-hv
cd score-hv
pip install .
```

### **2. Create the .env file**

The user must define a '.env-*' file in score-monitoring containing the 
appropriate information based on .env-example. The name of the file may be 
anything starting with .env-{user's choice}. This name will be passed into the 
code as a variable in the next step. This information will be referenced 
throughout the suite as needed. 

.env-* file format: 
```
EXPERIMENT_NAME = 'EXAMPLE-EXPERIMENT-NAME'
EXPERIMENT_WALLCLOCK_START = '2023-01-22 09:22:05'
STORAGE_LOCATION_BUCKET = 's3-bucket'
STORAGE_LOCATION_PLATFORM = 'aws_s3'
STORAGE_LOCATION_KEY = 'location/date_format/sub-directories' # no trailing "/"
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
GSI_FIT_FILE_NAME_FORMAT = 'gsistats.%Y%m%d%H_control'
```

EXPERIMENT_NAME and EXPERIMENT_WALLCLOCK_START are user defined values which 
are used for registering experiments and then referencing that experiment when 
storing other data, including file counts and metrics. Once registered, these 
values need to stay consistent.
STORAGE_LOCATION_BUCKET is the root name of the S3 bucket, this must match what 
is in AWS.
STORAGE_LOCATION_PLATFORM is a metadata value used for referencing the storage 
location for registration and file counts.
STORAGE_LOCATION_KEY is the key location in the S3 bucket beneath the root to 
be used. This will be used for metadata registration and pulling data. The 
value can be an empty string if the top of the S3 bucket is being used and no 
'/' should be in the front of the key or at the end of the key. It should 
include any year, month, and date string replacement values using standard 
format codes (e.g., %Y/%m/%Y%m%d%H for the string formatted as 
[YYYY]/[MM]/[YYYYMMDDHH]). Documentation for Python's datetime format codes are 
provided at 
[https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes).

### **3. Copy python scripts into workflow directory**
The cylc workflow script calls other python scripts (stored in the scripts 
directory), which contain lower level calls to score-hv and to score-db. To
make a workflow aware of these scripts, first you need to copy them into a bin/ 
folder under a workflow directory. The "install_scripts.sh" bash tool can
be used to safely copy these files to a specified workflow directory.

```
./install_scripts.sh PATH-TO-WOFKLOW-DIR
```

### **4. Modify the cylc workflow**
The cylc workflow script has four pieces of information that the user should define: 
email, cycle times, .env-* file name, and (if desired) stats to run data 
collection on. These values are found in (or just below) the parameters section 
of the top of the *flow.cylc* file, which is located under a cylc workflow 
directory. An example workflow directory is provided at cylc8_test_flow.

```
# parameters 
{% set MAIL_ADDRESS = 'first.last@noaa.gov' %}
{% set INITIAL_CYCLE_POINT = '20090501T06' %}
{% set FINAL_CYCLE_POINT = '20090503T18' %}
{% set ENV_PATH = '../.env-example' %}
```

Modify each of these values to be appropriate for your user and experiment. The 
email address is used to notify the user of task failures. The ENV_PATH value 
should be relative to the locaion of the *flow.cylc* file, likely starting with 
'../.env-*' if following the example pattern and location of .env-example. 

If desired, the user can change with stats will be stored based on the 
paramater 'stats' found under '[task parameters]'. By default, this list is 
already updated with all current values available. stat values must be 
associated with a script of the form db_{stat}.py in the scripts folder to be 
valid. All values listed under stats will be stored via the store_data step of 
the cylc graph.

```
[task parameters]
	stats = file_count, gsi_obsfit
```

### **5. Register relevant information in the database**

If necessary, information may need to be pre-registered into the database for 
your cylc suite to store values correctly. Values which must be pre-regsitered 
in the database include metadata abbout experiments, storage locations, file 
types, and metric types. Each item must only be registered once and does not 
need to be re-registered for each run. 

To register values, update the appropriate function in *db-registration.py* and 
run the script using python3. Values which need to be updated by the user are 
flagged with a comment *#USER DEFINED VARIABLES* and closed with *#END USER 
DEFINED VARIABLES*.  

Experiment metadata will need to be registered once before storing any other 
data connected to the experiment. Experiment name and wallclock start are used 
as references and are pulled from the .env file for consistency.

Storage location metadata will need to registered when using a new bucket for 
the first time or a different key within a bucket for the first time, i.e. if 
changing any of the values referenced in the .env instead of the script.

File type metadata will need to be registered if a different file type is being 
used for the file count script.

Metric type metadata will need to be registered when harvesting new metrics, 
such as using the inc_logs script or when a new harvester functionality is 
added.

Depending on which values need to be registered, the user must first 
comment/uncomment function calls in the main() function as found below: 

```
    #USER SHOULD COMMENT / UNCOMMENT CALLS AS APPROPRIATE
    register_experiment("USER DEFINED INPUT FOR EXPERIMENT DESCRIPTION")
    register_storage_location()
    register_file_type()
    register_metric_type()
```

In order to run the script, the anaconda3 module must be active. If needed, 
please see the steps to load modules in step 6. 

Registration is run by calling the db-registration.py script with the .env file 
written in step 2 in place of the .env-example.

```
python db-registration.py ../.env-example
```

## Running a Workflow 

### **6. Install the workflow** 

Install a workflow by running the 'install' command after confirming the 
flow.cylc script is valid. 

```
cylc validate PATH-TO-WORKFLOW-DIRECTORY
cylc install PATH-TO-WORKFLOW-DIRECTORY
```

### **7. Run the workflow**
Once installed, a workflow can be run using cylc commands. For the full list of 
commands to use, see the Cylc documentation.

```
cylc play WORKFLOW
```

### **8. Monitor the workflow**
While a workflow is running, you have the option to monitor the process using 
cylc's terminal user interface, or by checking the files in the job output 
folders. 

```
cylc tui WORKFLOW
```

### **9. Handling failures**
Some failures are expected in the design of a workflow, particularly if files 
have not populated in the source storage location. 

If the storage location does not contain any files or if any of the files are 
less than 30 minutes old, the FILE CHECK task will purposely fail and retry. 
This will continue for a specified number of trys to allow files to populate 
over time as necessary before the suite will completely fail. 

If one of the store data tasks fails, they will also retry. If the calls to 
score-db fail, then the task will also fail. 

If a task fails, the job.out and job.err file outputs in the cylc work output 
folders will contain print out data that can be useful in diagnosing additional 
issues. You can read the logs under the cylc-run directory under the name of 
the example-suite.

For example, looking up the logs of the file_check would be under:

```
cd $HOME/cylc-run/WORKFLOW/run1/log/job/CYCLE_TIME/file_check/NN/
cat job.out
cat job.err
```
where CYCLE_TIME is the cycle you'd like to see such as 20050101T00.

### **10. Stop the workflow**
If you need to stop a workflow while it's running, you can call the cylc stop 
command. 

```
cylc stop WORKFLOW
```

If you need a workflow to stop immediately and stop any running tasks at the 
--now flags. One --now will stop after the task completes and two --now --now 
flags will interrupt the currently running task to stop immediately.

```
cylc stop --now --now WORKFLOW
```