#!/bin/bash
set -e

source ./pydatatable/py-pydatatable/bin/activate
python -c 'import datatable as dt; open("pydatatable/VERSION","w").write(dt.__version__); open("pydatatable/REVISION","w").write(dt.__git_revision__);' > /dev/null
