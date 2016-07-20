#!/bin/bash
set -e

# Required only on client machine

# install python, pandas and pydoop.hdfs
apt-get update
apt-get install build-essential python-dev python-pip
pip install --upgrade pandas
pip install pydoop.hdfs

# check
python
import pandas as pd
pd.__version__
