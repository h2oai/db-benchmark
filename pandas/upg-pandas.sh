#!/bin/bash
set -e

echo 'upgrading pandas...'

source ./pandas/py-pandas/bin/activate

python3 -m pip install --upgrade pandas > /dev/null
