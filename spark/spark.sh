#!/bin/bash
set -e

# init
./spark/init-spark.sh


# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  while read line
  do 
    eval $line
    $SPARK_HOME/bin/spark-shell --master $SPARK_MASTER --verbose --packages com.databricks:spark-csv_2.10:1.3.0 --num-executors 9 --executor-memory 200g --driver-memory 200g --conf spark.driver.maxResultSize=200g --conf spark.network.timeout=2400 --conf spark.executor.heartbeatInterval=1200 -i ./spark/join-spark.scala
  done < ./loop-join-data.env
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do 
    eval $line
    $SPARK_HOME/bin/spark-shell --master $SPARK_MASTER --verbose --packages com.databricks:spark-csv_2.10:1.3.0 --num-executors 9 --executor-memory 200g --driver-memory 200g --conf spark.driver.maxResultSize=200g --conf spark.network.timeout=2400 --conf spark.executor.heartbeatInterval=1200 -i ./spark/groupby-spark.scala
  done < ./loop-groupby-data.env
fi

# shutdown
./spark/shutdown-spark.sh
