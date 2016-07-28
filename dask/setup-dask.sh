#!/bin/bash
set -e

# install on client node
python -m pip install --user -U dask[complete]
python -m pip install --user -U distributed

# scheduler node mr-0xd8 for web uo
python -m pip install --user -U bokeh

# install/upgrade to user library (python -m ensure python 2.7)
for i in $CLUSTER; do cmd="ssh $USER@$i 'python -m pip install --user -U distributed'"; echo $cmd; eval $cmd; done
for i in $CLUSTER; do cmd="ssh $USER@$i 'mkdir -p ~/tmp/dask'"; echo $cmd; eval $cmd; done

# ls -la .local/lib/python2.7/site-packages/
# ls -la .local/lib/python3.5/site-packages/
#for i in $CLUSTER; do cmd="ssh $USER@$i 'pip install --user -U dask[complete]'"; echo $cmd; eval $cmd; done
