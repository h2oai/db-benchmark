#!/bin/bash
set -e

echo 'upgrading dask...'

source ./dask/py-dask/bin/activate

python -m pip install --upgrade dask > /dev/null
