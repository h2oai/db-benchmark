#!/bin/bash
set -e

# install all dependencies
sudo apt-get update
sudo apt-get install build-essential 

virtualenv dask/py-dask --python=/usr/bin/python3.10
source dask/py-dask/bin/activate

# install binaries
python3 -m pip install --upgrade dask[complete]

# check
python3
import dask as dk
dk.__version__
dk.__git_revision__
quit()

deactivate
