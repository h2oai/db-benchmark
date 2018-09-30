#!/bin/bash
set -e

# install all dependencies
#apt-get update
#apt-get install build-essential python3-dev python3-pip

virtualenv dask/py-dask --python=/usr/bin/python3.6
source dask/py-dask/bin/activate

# install binaries
python -m pip install --upgrade dask
python -m pip install --upgrade dask[dataframe]
python -m pip install --upgrade dask[distributed]
#python -m pip install --upgrade fastparquet # not used as slower, at least for 1e7, 1e8. parquet portability issue spark-fastparquet prevent to try 1e9

# check
python
import dask as dk
dk.__version__
dk.__git_revision__
quit()

deactivate
