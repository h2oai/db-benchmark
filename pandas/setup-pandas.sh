#!/bin/bash
set -e

# install all dependencies
sudo apt-get update
sudo apt-get install build-essential python3-dev python3-pip

virtualenv pandas/py-pandas --python=/usr/bin/python3.7
source pandas/py-pandas/bin/activate

# install binaries
python3 -m pip install --upgrade psutil
python3 -m pip install --upgrade pandas
python3 -m pip install git+https://github.com/h2oai/datatable

# install datatable for fast data import
python3 -m pip install --upgrade datatable

# check
python3
import pandas as pd
pd.__version__
quit()
deactivate
