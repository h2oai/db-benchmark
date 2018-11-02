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
  Rscript ./log-task-solution.R groupby spark 0
  while read line
  do 
    eval $line
    ./spark/groupby-spark.py
  done < ./loop-groupby-data.env
  Rscript ./log-task-solution.R groupby spark 1
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
