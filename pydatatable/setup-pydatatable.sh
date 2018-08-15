#!/bin/bash
set -e

cd pydatatable
virtualenv py-pydatatable --python=/usr/bin/python3.6
cd ..
source pydatatable/py-pydatatable/bin/activate

# install all dependencies

# os spec
wget https://releases.llvm.org/6.0.0/clang+llvm-6.0.0-x86_64-linux-gnu-ubuntu-16.04.tar.xz
mv clang+llvm-6.0.0-x86_64-linux-gnu-ubuntu-16.04.tar.xz  /opt
cd /opt
sudo tar xvf clang+llvm-6.0.0-x86_64-linux-gnu-ubuntu-16.04.tar.xz
export LLVM6=/opt/clang+llvm-6.0.0-x86_64-linux-gnu-ubuntu-16.04/
#export LDFLAGS="-L/usr/lib/gcc/x86_64-linux-gnu/7/"

#apt-get install python3.6-dev
python -m pip install --upgrade llvmlite

# build
./pydatatable/init-pydatatable.sh

#python -m pip install https://s3.amazonaws.com/h2o-release/datatable/stable/datatable-0.5.0/datatable-0.5.0-cp35-cp35m-linux_x86_64.whl

# check
python
import datatable as dt
dt.__version__
quit()
