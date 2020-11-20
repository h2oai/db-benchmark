#!/bin/bash
set -e

# install dependencies
sudo apt-get update -qq
sudo apt-get install -y python3.6-dev virtualenv

virtualenv pydatatable/py-pydatatable --python=/usr/bin/python3.6
source pydatatable/py-pydatatable/bin/activate

python -m pip install --upgrade psutil
python -m pip install --upgrade pyarrow

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

# resave 1e9 join data from csv to jay format so pydt can try out-of-memory processing
source pydatatable/py-pydatatable/bin/activate
python
import datatable as dt
dt.Fread('data/J1_1e9_NA_0_0.csv').to_jay('data/J1_1e9_NA_0_0.jay')
dt.Fread('data/J1_1e9_1e9_0_0.csv').to_jay('data/J1_1e9_1e9_0_0.jay')
dt.Fread('data/J1_1e9_1e6_0_0.csv').to_jay('data/J1_1e9_1e6_0_0.jay')
dt.Fread('data/J1_1e9_1e3_0_0.csv').to_jay('data/J1_1e9_1e3_0_0.jay')
quit()
deactivate
