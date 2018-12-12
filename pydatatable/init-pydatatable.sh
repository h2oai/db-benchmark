#!/bin/bash
set -e

echo 'upgrading pydatatable...'

source ./pydatatable/py-pydatatable/bin/activate
rm -rf ./tmp/datatable
# python 'import datatable as dt; if (system("git rev-parse HEAD") == dt.__git_revision__) exit()'
mkdir -p ./tmp/datatable

git clone --depth=1 https://github.com/h2oai/datatable.git ./tmp/datatable &> /dev/null
cd tmp/datatable

export LLVM6=/opt/clang+llvm-6.0.1-x86_64-linux-gnu-ubuntu-16.04/
export LDFLAGS="-L/usr/lib/gcc/x86_64-linux-gnu/8/" # assuming your gcc uses gcc-8 and gcc-8-multilib g++-8-multilib installed

make clean > /dev/null
make build &> /dev/null
make install > /dev/null

cd ../..
rm -rf ./tmp/datatable
