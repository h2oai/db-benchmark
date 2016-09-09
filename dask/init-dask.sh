#!/bin/bash
set -e

[ -z "$CLUSTER" ] && echo "Need to set CLUSTER" && exit 1;

# kill all dask actors
(pkill -9 dask-scheduler && pkill -9 bokeh) || true
for i in $SLAVES; do (ssh $USER@$i "killall -9 dask-worker 2>&1 > /dev/null" &) 2>&1 > /dev/null; done && sleep 5

# read this before modifying https://github.com/conda/conda/issues/448

echo "Starting dask scheduler..." # web ui: http://$MASTER:18787/
export PYTHONNOUSERSITE=True PATH=$HOME/miniconda2/bin:$PATH; nohup $HOME/miniconda2/bin/dask-scheduler --bokeh-port 18787 > ~/tmp/dask/scheduler.log 2>&1 &

echo "Starting dask cluster..."
for i in $SLAVES; do (ssh $USER@$i "export PYTHONNOUSERSITE=True PATH=$HOME/miniconda2/bin:\$PATH; nohup $HOME/miniconda2/bin/dask-worker $MASTER:8786 > ~/tmp/dask/worker.log 2>&1" & ); done && sleep 2 && echo Started dask cluster ok on $SLAVES
sleep 10
