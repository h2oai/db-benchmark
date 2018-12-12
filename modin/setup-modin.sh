#!/bin/bash
set -e

virtualenv modin/py-modin --python=/usr/bin/python3.6
source modin/py-modin/bin/activate

# install binaries
python -m pip install --upgrade modin

# check
python
import modin as pd
pd.__version__
quit()

deactivate
