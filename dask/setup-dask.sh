#!/bin/bash
set -e

# install all dependencies
sudo apt-get update
sudo apt-get install build-essential python3-dev python3-pip

virtualenv dask/py-dask --python=/usr/bin/python3.6
source dask/py-dask/bin/activate

# install binaries
python -m pip install --upgrade dask
python -m pip install --upgrade dask[dataframe]
python -m pip install --upgrade dask[distributed]
python -m pip install --upgrade pyarrow

# check
python
import dask as dk
dk.__version__
dk.__git_revision__
quit()

deactivate
