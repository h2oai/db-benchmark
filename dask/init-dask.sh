#!/bin/bash
set -e

[ -z "$CLUSTER" ] && echo "Need to set CLUSTER" && exit 1;

# kcl: always kill all hosts to not leave lying around when changing configs
for i in $CLUSTER; do (ssh $USER@$i "killall -9 python 2>&1 > /dev/null" &) 2>&1 > /dev/null; done && sleep 5

# start cluster
echo "Starting cluster..."
nohup ~/.local/bin/dask-ssh $CLUSTER >  ~/tmp/dask/cluster.log 2>&1 &
