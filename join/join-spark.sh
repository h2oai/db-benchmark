#!/bin/bash
set -e

## Requirements:
# 1. passwordless ssh from mr-0xd6 to mr-0xd1:10
# 2. run one-time setup-spark.sh to copy spark binaries, etc.
# 3. optionally change $SPARK_MASTER_PORT to ensure to have private master
# 4. exported vars SRC_X and SRC_Y, see join.sh

# configure spark env vars and start cluster
export SPARK_PRINT_LAUNCH_COMMAND=1
export SPARK_MASTER_IP="mr-0xd8"
export SPARK_MASTER_PORT="17077"
export SPARK_WORKER_IP="mr-0xd1 mr-0xd2 mr-0xd3 mr-0xd4 mr-0xd5 mr-0xd7 mr-0xd9 mr-0xd10"
export SPARK_MASTER="spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"
export SPARK_HOME="$HOME/spark-2.0.0-SNAPSHOT-bin-hadoop2.6"
export SPARK_LOG_DIR=~/tmp/spark/logs # not /tmp as that's limited to 50GB and got full with 1e9 test: 'No space left on device'
export SPARK_WORKER_DIR=~/tmp/spark/work
export SPARK_LOCAL_DIRS=~/tmp/spark/work

rm -rf ~/tmp/spark # cleanup in case previous

echo "Starting master..."
cmdall="mkdir -p ~/tmp/spark; rm -rf ~/tmp/spark; export SPARK_MASTER_PORT=$SPARK_MASTER_PORT; export SPARK_LOG_DIR=~/tmp/spark/logs; export SPARK_WORKER_DIR=~/tmp/spark/work; export SPARK_LOCAL_DIRS=~/tmp/spark/work; export SPARK_HOME='$SPARK_HOME'; export SPARK_MASTER='$SPARK_MASTER'; $SPARK_HOME/sbin/start-master.sh"
cmd="ssh $USER@$SPARK_MASTER_IP \""$cmdall"\"";
echo $cmd
eval $cmd
sleep 10

echo "Starting workers..."
cmdall="mkdir -p ~/tmp/spark; rm -rf ~/tmp/spark; export SPARK_MASTER_PORT=$SPARK_MASTER_PORT; export SPARK_LOG_DIR=~/tmp/spark/logs; export SPARK_WORKER_DIR=~/tmp/spark/work; export SPARK_LOCAL_DIRS=~/tmp/spark/work; export SPARK_HOME='$SPARK_HOME'; export SPARK_MASTER='$SPARK_MASTER'; $SPARK_HOME/sbin/start-slave.sh $SPARK_MASTER"
for i in $SPARK_WORKER_IP; do cmd="ssh $USER@$i \""$cmdall"\""; echo $cmd; eval $cmd; done
sleep 15

# execute scala benchmark script
$SPARK_HOME/bin/spark-shell --master $SPARK_MASTER --packages com.databricks:spark-csv_2.10:1.3.0 --num-executors 9 --executor-memory 220G --driver-memory 220G --conf spark.driver.maxResultSize=0 -i join-spark.scala
sleep 5

# stop workers
echo "Stopping workers..."
cmdall="$SPARK_HOME/sbin/stop-slave.sh $SPARK_MASTER"
for i in $SPARK_WORKER_IP; do cmd="ssh $USER@$i \""$cmdall"\""; echo $cmd; eval $cmd; done;
sleep 5;

# stop master
echo "Stopping master..."
cmdall="$SPARK_HOME/sbin/stop-master.sh";
cmd="ssh $USER@$SPARK_MASTER_IP \""$cmdall"\"";
echo $cmd;
eval $cmd;
