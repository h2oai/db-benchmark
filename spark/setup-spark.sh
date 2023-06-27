#!/bin/bash
set -e

# install java8
sudo apt-get install openjdk-8-jdk

virtualenv spark/py-spark --python=python3


# put to paths
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> path.env

source path.env

source spark/py-spark/bin/activate
# install binaries
python3 -m pip install --upgrade psutil
python3 -m pip install --upgrade pyspark

# check
# python3
# import pyspark
# pyspark.__version__
# quit()

deactivate
