#!/bin/bash --posix

module use /contrib/home/builder/UFS-RNR-stack/modules
module load anaconda3
echo 'modules loaded'
which python3

SCORE_DB_HOME_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PYTHONPATH=$SCORE_DB_HOME_DIR/src
echo PYTHONPATH=$PYTHONPATH
echo Python Version: $(python --version)
