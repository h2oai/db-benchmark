#!/bin/bash
set -e

echo 'upgrading datafusion...'

source ./datafusion/py-datafusion/bin/activate

python -m pip install --upgrade datafusion > /dev/null