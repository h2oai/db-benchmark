#!/bin/bash
set -e

# init
./spark/init-spark.sh

# join
$SPARK_HOME/bin/spark-shell --master $SPARK_MASTER --packages com.databricks:spark-csv_2.10:1.3.0 --num-executors 9 --executor-memory 200g --driver-memory 200g --conf spark.driver.maxResultSize=200g --conf spark.network.timeout=1800 --conf spark.executor.heartbeatInterval=600 -i ./spark/join-spark.scala

# shutdown
./spark/shutdown-spark.sh
