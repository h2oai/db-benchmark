#!/bin/bash
set -e

echo 'upgrading dask...'

source ./dask/py-dask/bin/activate

python -m pip install --upgrade dask > /dev/null

python -c 'import dask as dk; open("dask/VERSION","w").write(dk.__version__); open("dask/REVISION","w").write(dk.__git_revision__);' > /dev/null
