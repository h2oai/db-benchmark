#!/bin/bash
set -e

[ -z "$SPARK_WORKER_IP" ] && echo "Need to set SPARK_WORKER_IP" && exit 1;

# stop workers
echo "Stopping workers..."
cmdall="$SPARK_HOME/sbin/stop-slave.sh $SPARK_MASTER"
for i in $SPARK_WORKER_IP; do cmd="ssh $USER@$i \""$cmdall"\""; eval $cmd; done;
sleep 5;

# stop master
echo "Stopping master..."
cmdall="$SPARK_HOME/sbin/stop-master.sh";
cmd="ssh $USER@$SPARK_MASTER_IP \""$cmdall"\""; eval $cmd;
