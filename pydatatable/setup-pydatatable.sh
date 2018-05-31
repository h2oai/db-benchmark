#!/bin/bash
set -e

virtualenv py35 --python=/usr/bin/python35
source py35/bin/activate

python -m pip install https://s3.amazonaws.com/h2o-release/datatable/stable/datatable-0.5.0/datatable-0.5.0-cp35-cp35m-linux_x86_64.whl

# check
python
import datatable as dt
dt.__version__
