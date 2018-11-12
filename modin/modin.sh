#!/bin/bash
set -e

source ./modin/py-modin/bin/activate

## join - NotImplementedError
#if [[ "$RUN_TASKS" =~ "join" ]]; then
#  while read line
#  do 
#    eval $line
#    ./modin/join-modin.py
#  done < ./loop-join-data.env
#fi

## groupby - NotImplementedError
#if [[ "$RUN_TASKS" =~ "groupby" ]]; then
#  while read line
#  do 
#    eval $line
#    ./modin/groupby-modin.py || true
#  done < ./loop-groupby-data.env
#fi

## sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R sort modin $SRC_X 0
    ./modin/sort-modin.py
    Rscript ./log-task-solution-data.R sort modin $SRC_X 1
  done < ./loop-sort-data.env
fi

## read
#if [[ "$RUN_TASKS" =~ "read" ]]; then
#  while read line
#  do 
#    eval $line
#    ./modin/read-modin.py
#  done < ./loop-read-data.env
#fi

