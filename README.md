# Monitoring Cylc Suite for Statistics 

Initial development focused on monitoring integration for the replay experiments

Development on this repo is ongoing and more features will be added. Currently, it is able to register database metadata and store all file counts for an aws s3 bucket via cylc cycles. 

This repo contains a cylc suite and python scripts. It is dependent on having access to the score-db and score-hv repositories. The score-hv repo should be connected to the score-db repo via the bash script score_db_utils.sh in the score-db repo. Then the location of the score-db repo should be specified prior to running the score-monitoring scripts via the environment variable SCORE_DB_BASE_LOCATION. 

## Cylc Suite
For the cylc suite, users should specify the parameters in the suite.rc file prior to runs configured to their use case. The cylc suite will cycle through a given time period and run a file check in aws s3 (via python script) against the bucket specified in the .env in the parameters. If the file check fails, finds no files, or finds files are less than 30 minutes old, the cylc task will fail and retry in 1 hour.  After the file check is done, additional tasks will run such as storing file counts or metrics as specified in the suite graph and stats parameters.


## Python Scripts
Most of the python scripts are called via the cylc suite, except the db-registration.py script which must be run independently by the user as necessary.

The db-registration script will store the provided metadata into the database for experiments, storage locations, and file types. This metadata is requird in the database to store file counts and metrics. Each experiment, storage location, and file type will only need to be registered once. Usage of the script is dependent on the users specific use case and must be edited for the user defined inputd prior to each run. It is also possible the user may only want to register one type of metadata and in those cases the other function calls should be commented out in the main() function. For example, if you only need to add a new file type, you'd edit the file type metadata in the register_file_type() function and then comment out the register_experiment() and register_storage_location() functions from the main() function "#register_experiment() and #register_storage_location()".