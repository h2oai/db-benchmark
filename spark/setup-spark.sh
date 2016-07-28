#!/bin/bash
set -e

## Spark 2.0

# https://spark.apache.org/downloads.html
wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.0-bin-hadoop2.6.tgz
md5sum spark-2.0.0-bin-hadoop2.6.tgz
# 6d9cac116f3434330869dedb6c5fe9ca  spark-2.0.0-bin-hadoop2.6.tgz
# http://www.apache.org/dist/spark/spark-2.0.0/spark-2.0.0-bin-hadoop2.6.tgz.md5
tar -xf spark-2.0.0-bin-hadoop2.6.tgz

export SPARK_HOME="$HOME/spark-2.0.0-bin-hadoop2.6"
export SPARK_WORKER_IP="mr-0xd1 mr-0xd2 mr-0xd3 mr-0xd4 mr-0xd5 mr-0xd7 mr-0xd8 mr-0xd9 mr-0xd10"

# copy spark binaries to nodes
for i in $SPARK_WORKER_IP; do cmd="rsync -aq $SPARK_HOME $USER@$i:."; echo $cmd; eval $cmd; done
# local tmp dir
for i in $SPARK_WORKER_IP; do cmd="ssh $USER@$i 'mkdir -p tmp'"; echo $cmd; eval $cmd; done





