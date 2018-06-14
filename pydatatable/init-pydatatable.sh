#!/bin/bash
set -e

echo 'upgrading pydatatable...'

# at this point python virtual env should be sourced and LLVM6 env var set

rm -rf ./tmp/datatable

# python 'import datatable as dt; if (system("git rev-parse HEAD") == dt.__git_revision__) exit()'

mkdir -p ./tmp/datatable

git clone --depth=1 https://github.com/h2oai/datatable.git ./tmp/datatable &> /dev/null
cd tmp/datatable
make clean > /dev/null
make build &> /dev/null
make install > /dev/null

cd ../..
rm -rf ./tmp/datatable
