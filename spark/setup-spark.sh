#!/bin/bash
set -e

# install java, setup JAVA_HOME

cd spark
virtualenv py-spark --python=/usr/bin/python3.6
cd ..
source spark/py-spark/bin/activate

# install binaries
python -m pip install --upgrade psutil
python -m pip install --upgrade pyspark

# check
python
import pyspark
pyspark.__version__
quit()
