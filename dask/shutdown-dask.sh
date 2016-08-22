#!/bin/bash
set -e

[ -z "$CLUSTER" ] && echo "Need to set CLUSTER" && exit 1;

echo "Stopping dask cluster..."
pkill -9 dask-ssh && sleep 5
for i in $CLUSTER; do (ssh $USER@$i "killall -9 python 2>&1 > /dev/null" &) 2>&1 > /dev/null; done && sleep 5
