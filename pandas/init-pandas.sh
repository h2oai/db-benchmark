#!/bin/bash
set -e

echo 'upgrading pandas...'

source ./pandas/py-pandas/bin/activate

python -m pip install --upgrade pandas > /dev/null
