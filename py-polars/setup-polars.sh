#!/bin/bash
set -e

# install dependencies
sudo apt-get update -qq
sudo apt-get install -y python3.6-dev virtualenv

virtualenv polars/py-polars --python=/usr/bin/python3.6
source polars/py-polars/bin/activate

python -m pip install --upgrade psutil py-polars

# build
deactivate
./polars/upg-polars.sh

# check
source polars/py-polars/bin/activate
python
import pypolars as pl
pl.__version__
quit()
deactivate

# fix: print(ans.head(3), flush=True): UnicodeEncodeError: 'ascii' codec can't encode characters in position 14-31: ordinal not in range(128)
vim polars/py-polars/bin/activate
#deactivate () {
#    unset PYTHONIOENCODING
#    ...
#}
#...
#PYTHONIOENCODING="utf-8"
#export PYTHONIOENCODING
#...
