#!/bin/bash
set -e

echo 'upgrading dask...'

source ./dask/py-dask/bin/activate

python3 -m pip install --upgrade dask[complete] > /dev/null
