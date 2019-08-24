#!/bin/bash
set -e

source ./modin/py-modin/bin/activate
python -c 'import modin as modin; open("modin/VERSION","w").write(modin.__version__); open("modin/REVISION","w").write("");' > /dev/null
