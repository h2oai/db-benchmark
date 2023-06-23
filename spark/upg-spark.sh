#!/bin/bash
set -e

echo 'upgrading spark...'

source ./spark/py-spark/bin/activate

python3 -m pip install --upgrade pyspark > /dev/null

deactivate