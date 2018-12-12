#!/bin/bash
set -e

# install dependencies
sudo apt-get update
sudo apt-get install python3.6-dev gcc-8-multilib # or other multilib, depends on your gcc version

virtualenv pydatatable/py-pydatatable --python=/usr/bin/python3.6
source pydatatable/py-pydatatable/bin/activate

# install llvm to build pydatatable
wget https://releases.llvm.org/6.0.1/clang+llvm-6.0.1-x86_64-linux-gnu-ubuntu-16.04.tar.xz
tar -xvf clang+llvm-6.0.1-x86_64-linux-gnu-ubuntu-16.04.tar.xz
sudo mv clang+llvm-6.0.1-x86_64-linux-gnu-ubuntu-16.04 /opt
rm clang+llvm-6.0.1-x86_64-linux-gnu-ubuntu-16.04.tar.xz

python -m pip install --upgrade llvmlite

# build
deactivate
./pydatatable/init-pydatatable.sh

# check
source pydatatable/py-pydatatable/bin/activate
python
import datatable as dt
dt.__version__
quit()
deactivate
