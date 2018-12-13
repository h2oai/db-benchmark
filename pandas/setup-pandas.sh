#!/bin/bash
set -e

# install all dependencies
sudo apt-get update
sudo apt-get install build-essential python3-dev python3-pip

virtualenv pandas/py-pandas --python=/usr/bin/python3.6
source pandas/py-pandas/bin/activate

# install binaries
python -m pip install --upgrade psutil
python -m pip install --upgrade pandas

# install datatable for fast data import from jay format
pip install https://s3.amazonaws.com/h2o-release/datatable/stable/datatable-0.7.0/datatable-0.7.0-cp36-cp36m-linux_x86_64.whl

# check
python
import pandas as pd
pd.__version__
quit()

deactivate
