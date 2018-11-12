#!/bin/bash
set -e

source ./dask/py-dask/bin/activate

## join
#if [[ "$RUN_TASKS" =~ "join" ]]; then
#  while read line
#  do 
#    eval $line
#    ./dask/join-dask.py
#  done < ./loop-join-data.env
#fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R groupby dask $SRC_GRP_LOCAL 0
    ./dask/groupby-dask.py || true
    Rscript ./log-task-solution-data.R groupby dask $SRC_GRP_LOCAL 1
  done < ./loop-groupby-data.env
fi

## sort
#if [[ "$RUN_TASKS" =~ "sort" ]]; then
#  while read line
#  do 
#    eval $line
#    ./dask/sort-dask.py
#  done < ./loop-sort-data.env
#fi

## read
#if [[ "$RUN_TASKS" =~ "read" ]]; then
#  while read line
#  do 
#    eval $line
#    ./dask/read-dask.py
#  done < ./loop-read-data.env
#fi
