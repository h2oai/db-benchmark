#!/bin/bash
set -e

# install dependencies
sudo apt-get update -qq
sudo apt-get install -y python3.6-dev virtualenv

virtualenv datafusion/py-datafusion --python=/usr/bin/python3.6
source datafusion/py-datafusion/bin/activate

python -m pip install --upgrade psutil datafusion

# build
deactivate
./datafusion/upg-datafusion.sh

# check
# source datafusion/py-datafusion/bin/activate
# python
# import datafusion as df
# df.__version__
# quit()
# deactivate
echo "0.4.0"

# fix: print(ans.head(3), flush=True): UnicodeEncodeError: 'ascii' codec can't encode characters in position 14-31: ordinal not in range(128)
vim datafusion/py-datafusion/bin/activate
#deactivate () {
#    unset PYTHONIOENCODING
#    ...
#}
#...
#PYTHONIOENCODING="utf-8"
#export PYTHONIOENCODING
#...
