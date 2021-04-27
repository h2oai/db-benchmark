#!/bin/bash
set -e

source ./polars/py-polars/bin/activate
python -c 'import polars as pl; open("polars/VERSION","w").write(pl.__version__); open("polars/REVISION","w").write("");' > /dev/null
