#!/bin/bash
set -e

source ./spark/py-spark/bin/activate
python -c 'import pyspark; open("spark/VERSION","w").write(pyspark.__version__); open("spark/REVISION","w").write("");' > /dev/null
