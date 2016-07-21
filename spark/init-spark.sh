#!/bin/bash
set -e

[ -z "$SPARK_WORKER_IP" ] && echo "Need to set SPARK_WORKER_IP" && exit 1;

rm -rf ~/tmp/spark # cleanup in case previous

echo "Starting master..."
cmdall="mkdir -p ~/tmp/spark; rm -rf ~/tmp/spark; export SPARK_MASTER_PORT=$SPARK_MASTER_PORT; export SPARK_LOG_DIR=~/tmp/spark/logs; export SPARK_WORKER_DIR=~/tmp/spark/work; export SPARK_LOCAL_DIRS=~/tmp/spark/work; export SPARK_HOME='$SPARK_HOME'; export SPARK_MASTER='$SPARK_MASTER'; $SPARK_HOME/sbin/start-master.sh"
cmd="ssh $USER@$SPARK_MASTER_IP \""$cmdall"\""; eval $cmd;
sleep 5

echo "Starting workers..."
cmdall="mkdir -p ~/tmp/spark; rm -rf ~/tmp/spark; export SPARK_MASTER_PORT=$SPARK_MASTER_PORT; export SPARK_LOG_DIR=~/tmp/spark/logs; export SPARK_WORKER_DIR=~/tmp/spark/work; export SPARK_LOCAL_DIRS=~/tmp/spark/work; export SPARK_HOME='$SPARK_HOME'; export SPARK_MASTER='$SPARK_MASTER'; $SPARK_HOME/sbin/start-slave.sh $SPARK_MASTER"
for i in $SPARK_WORKER_IP; do cmd="ssh $USER@$i \""$cmdall"\""; eval $cmd; done;
sleep 15
