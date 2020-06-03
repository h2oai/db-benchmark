#!/bin/bash
set -e

virtualenv modin/py-modin --python=/usr/bin/python3.6
source modin/py-modin/bin/activate

# install binaries
python -m pip install --upgrade modin[all]

# check
python
import modin
modin.__version__
quit()

deactivate
