#!/bin/bash
set -e

echo 'upgrading polars...'

source ./polars/py-polars/bin/activate

python -m pip install --upgrade polars > /dev/null
