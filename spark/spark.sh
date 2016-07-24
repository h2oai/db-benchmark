#!/bin/bash
set -e

# init
./spark/init-spark.sh

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
   $SPARK_HOME/bin/spark-shell --master $SPARK_MASTER --packages com.databricks:spark-csv_2.10:1.3.0 --num-executors 9 --executor-memory 200g --driver-memory 200g --conf spark.driver.maxResultSize=200g --conf spark.network.timeout=1800 --conf spark.executor.heartbeatInterval=600 -i ./spark/join-spark.scala
fi;

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  $SPARK_HOME/bin/spark-shell --master $SPARK_MASTER --packages com.databricks:spark-csv_2.10:1.3.0 --num-executors 9 --executor-memory 200g --driver-memory 200g --conf spark.driver.maxResultSize=200g --conf spark.network.timeout=1800 --conf spark.executor.heartbeatInterval=600 -i ./spark/groupby-spark.scala
fi;

# shutdown
./spark/shutdown-spark.sh
