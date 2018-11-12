#!/bin/bash
set -e

source ./spark/py-spark/bin/activate

## join
#if [[ "$RUN_TASKS" =~ "join" ]]; then
#  while read line
#  do 
#    eval $line
#    ./spark/join-spark.py
#  done < ./loop-join-data.env
#fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R groupby spark $SRC_GRP_LOCAL 0
    ./spark/groupby-spark.py
    Rscript ./log-task-solution-data.R groupby spark $SRC_GRP_LOCAL 1
  done < ./loop-groupby-data.env
fi

## sort
#if [[ "$RUN_TASKS" =~ "sort" ]]; then
#  while read line
#  do 
#    eval $line
#    ./spark/sort-spark.py
#  done < ./loop-sort-data.env
#fi

## read
#if [[ "$RUN_TASKS" =~ "read" ]]; then
#  while read line
#  do 
#    eval $line
#    ./spark/read-spark.py
#  done < ./loop-read-data.env
#fi
