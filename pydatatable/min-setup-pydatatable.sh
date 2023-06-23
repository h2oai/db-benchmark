#!/bin/bash
set -e

# install dependencies
sudo apt-get update -qq
sudo apt-get install -y python3.10 virtualenv

virtualenv pydatatable/py-pydatatable --python=/usr/bin/python3.10
source pydatatable/py-pydatatable/bin/activate

python -m pip install --upgrade psutil

# build
deactivate

./pydatatable/upg-pydatatable.sh
