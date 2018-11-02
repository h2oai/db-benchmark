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
  Rscript ./log-task-solution.R sort modin 0
  while read line
  do 
    eval $line
    ./modin/sort-modin.py
  done < ./loop-sort-data.env
  Rscript ./log-task-solution.R sort modin 1
fi

## read
#if [[ "$RUN_TASKS" =~ "read" ]]; then
#  while read line
#  do 
#    eval $line
#    ./modin/read-modin.py
#  done < ./loop-read-data.env
#fi

