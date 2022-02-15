#!/bin/bash
set -e

echo 'upgrading vaex...'

source ./vaex/py-vaex/bin/activate

python -m pip install -U vaex-core vaex-hdf5 > /dev/null
