#!/bin/bash
set -e

# install java8
sudo apt-get install openjdk-8-jdk

virtualenv spark/py-spark --python=/usr/bin/python3.6
source spark/py-spark/bin/activate

# put to paths
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> path.env

source path.env

# install binaries
python -m pip install --upgrade psutil
python -m pip install --upgrade pyspark

# check
python
import pyspark
pyspark.__version__
quit()

deactivate
