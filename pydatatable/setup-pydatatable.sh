#!/bin/bash
set -e

# install dependencies
sudo apt-get update -qq
sudo apt-get install -y python3.6-dev virtualenv

virtualenv pydatatable/py-pydatatable --python=/usr/bin/python3.6
source pydatatable/py-pydatatable/bin/activate

python -m pip install --upgrade psutil

# build
deactivate
./pydatatable/upg-pydatatable.sh

# check
source pydatatable/py-pydatatable/bin/activate
python
import datatable as dt
dt.__version__
quit()
deactivate
