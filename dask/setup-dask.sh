#!/bin/bash
set -e

# pkgs on master node
PATH=$HOME/miniconda2/bin:$PATH conda install dask distributed -c conda-forge
PATH=$HOME/miniconda2/bin:$PATH conda install hdfs3 -c conda-forge
PATH=$HOME/miniconda2/bin:$PATH conda install numpy -c conda-forge

## refresh miniconda on slaves
# for i in $SLAVES; do cmd="ssh $USER@$i rm -rf ~/miniconda2"; echo $cmd; eval $cmd; done
for i in $SLAVES; do cmd="rsync -aq ~/miniconda2 $USER@$i:."; echo $cmd; eval $cmd; done

# delete packages from miniconda?
# cd ~/miniconda2/pkgs
# ls | grep dask | xargs rm -rf
# ls | grep distributed | xargs rm -rf
# cd ~/miniconda2/bin
# ls | grep dask | xargs rm -rf

#### the old way:

#for i in $CLUSTER; do (ssh $USER@$i "killall -9 python 2>&1 > /dev/null" &) 2>&1 > /dev/null; done && sleep 5

# # start cluster
# echo "Starting dask cluster..."
# nohup ~/.local/bin/dask-ssh $CLUSTER >  ~/tmp/dask/cluster.log 2>&1 &
# sleep 15

# # install on client node
# python -m pip install --user -U dask[complete]
# python -m pip install --user -U distributed
# 
# # scheduler node mr-0xd8 for web ui - doesnt work
# # python -m pip install --user -U bokeh
# 
# # install/upgrade to user library (python -m ensure python 2.7)
# for i in $CLUSTER; do cmd="ssh $USER@$i 'python -m pip install --user -U distributed'"; echo $cmd; eval $cmd; done
# for i in $CLUSTER; do cmd="ssh $USER@$i 'mkdir -p ~/tmp/dask'"; echo $cmd; eval $cmd; done
