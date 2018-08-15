#!/bin/bash
set -e

# install all dependencies
apt-get update
apt-get install build-essential python3-dev python3-pip

cd pandas
virtualenv py-pandas --python=/usr/bin/python3.6
cd ..
source pandas/py-pandas/bin/activate

# build
#./pandas/init-pandas.sh

# install binaries
python -m pip install --upgrade pandas

# check
python
import pandas as pd
pd.__version__
quit()
