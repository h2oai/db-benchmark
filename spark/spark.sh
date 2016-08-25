#!/bin/bash
set -e

# init
./spark/init-spark.sh


# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  while read line
  do 
    eval $line
    $SPARK_HOME/bin/spark-shell --master $SPARK_MASTER --verbose --num-executors 9 --executor-memory 200g --driver-memory 200g --conf spark.driver.maxResultSize=200g --conf spark.network.timeout=2400 --conf spark.executor.heartbeatInterval=1200 -i ./spark/join-spark.scala
  done < ./loop-join-data.env
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do 
    eval $line
    $SPARK_HOME/bin/spark-shell --master $SPARK_MASTER --verbose --num-executors 9 --executor-memory 200g --driver-memory 200g --conf spark.driver.maxResultSize=200g --conf spark.network.timeout=2400 --conf spark.executor.heartbeatInterval=1200 -i ./spark/groupby-spark.scala
  done < ./loop-groupby-data.env
fi

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  while read line
  do
    eval $line
    $SPARK_HOME/bin/spark-shell --master $SPARK_MASTER --verbose --num-executors 9 --executor-memory 200g --driver-memory 200g --conf spark.driver.maxResultSize=200g --conf spark.network.timeout=2400 --conf spark.executor.heartbeatInterval=1200 -i ./spark/sort-spark.scala
  done < ./loop-sort-data.env
fi

# shutdown
./spark/shutdown-spark.sh
