#!/bin/bash
set -e

echo 'upgrading pydatatable...'

source ./pydatatable/py-pydatatable/bin/activate
pip install --upgrade git+https://github.com/h2oai/datatable > /dev/null 2>&1
deactivate
