#!/bin/bash
set -e

source ./pandas/py-pandas/bin/activate
python3 -c 'import pandas as pd; open("pandas/VERSION","w").write(pd.__version__); open("pandas/REVISION","w").write(pd.__git_version__);' > /dev/null
