#!/bin/bash
set -e

echo 'upgrading modin...'

source ./modin/py-modin/bin/activate

python -m pip install --upgrade modin > /dev/null
