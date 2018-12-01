#!/bin/bash
set -e

echo 'upgrading spark...'

source ./spark/py-spark/bin/activate

python -m pip install --upgrade pyspark > /dev/null

python -c 'import pyspark; open("spark/VERSION","w").write(pyspark.__version__); open("spark/REVISION","w").write("");' > /dev/null
