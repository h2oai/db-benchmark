#!/bin/bash
set -e

# stop workers
echo "Stopping workers..."
cmdall="$SPARK_HOME/sbin/stop-slave.sh $SPARK_MASTER"
for i in $SPARK_WORKER_IP; do cmd="ssh $USER@$i \""$cmdall"\""; eval $cmd; done;
sleep 5;

# stop master
echo "Stopping master..."
cmdall="$SPARK_HOME/sbin/stop-master.sh";
cmd="ssh $USER@$SPARK_MASTER_IP \""$cmdall"\""; eval $cmd;
