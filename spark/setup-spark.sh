#!/bin/bash
set -e

cd spark
virtualenv py-spark --python=/usr/bin/python3.6
cd ..
source spark/py-spark/bin/activate

# install binaries
python -m pip install --upgrade pyspark

# check
python
import pyspark as spark
spark.__version__
quit()
