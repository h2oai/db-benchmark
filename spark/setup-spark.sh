#!/bin/bash
set -e

## Requirements:
# 1. on mr-0xd6 you should have ~/spark-2.0.0-SNAPSHOT-bin-hadoop2.6

export SPARK_HOME="$HOME/spark-2.0.0-SNAPSHOT-bin-hadoop2.6"
export SPARK_WORKER_IP="mr-0xd1 mr-0xd2 mr-0xd3 mr-0xd4 mr-0xd5 mr-0xd7 mr-0xd8 mr-0xd9 mr-0xd10"

# copy spark binaries to nodes
for i in $SPARK_WORKER_IP; do cmd="rsync -aq $SPARK_HOME $USER@$i:."; echo $cmd; eval $cmd; done
# local tmp dir
for i in $SPARK_WORKER_IP; do cmd="ssh $USER@$i 'mkdir -p tmp'"; echo $cmd; eval $cmd; done
