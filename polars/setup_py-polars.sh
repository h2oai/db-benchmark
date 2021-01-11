#!/bin/bash
set -e

# install dependencies
sudo apt-get update -qq
sudo apt-get install -y python3.6-dev virtualenv

virtualenv polars/py-polars --python=/usr/bin/python3.6
source polars/py-polars/bin/activate

python -m pip install --upgrade psutil py-polars

# build
deactivate
./pydatatable/upg-pydatatable.sh

# check
source pydatatable/py-pydatatable/bin/activate
python
import pypolars as pl
pl.__version__
quit()
deactivate

