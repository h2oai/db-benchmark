#!/bin/bash
set -e

echo 'upgrading pandas...'

source ./pandas/py-pandas/bin/activate

python -m pip install --upgrade pandas > /dev/null

python -c 'import pandas as pd; open("pandas/VERSION","w").write(pd.__version__); open("pandas/REVISION","w").write("");' > /dev/null 2>1& # from 0.24.0 also revision
