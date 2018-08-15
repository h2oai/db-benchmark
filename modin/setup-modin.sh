#!/bin/bash
set -e

# install all dependencies
apt-get update
apt-get install build-essential python3-dev python3-pip

cd modin
virtualenv py-modin --python=/usr/bin/python3.6
cd ..
source modin/py-modin/bin/activate

# build
#./modin/init-modin.sh

# install binaries
python -m pip install --upgrade modin

# check
python
import modin as pd
pd.__version__
quit()
