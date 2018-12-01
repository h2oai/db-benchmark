#!/bin/bash
set -e

echo 'upgrading modin...'

source ./modin/py-modin/bin/activate

python -m pip install --upgrade modin > /dev/null

python -c 'import modin as modin; open("modin/VERSION","w").write(modin.__version__); open("modin/REVISION","w").write(modin.__git_revision__);' > /dev/null
