#!/bin/bash
set -e

source ./pydatatable/py-pydatatable/bin/activate

## join
#if [[ "$RUN_TASKS" =~ "join" ]]; then
#  while read line
#  do 
#    eval $line
#    ./pydatatable/join-pydatatable.py
#  done < ./loop-join-data.env
#fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  Rscript ./log-task-solution.R groupby pydatatable 0
  while read line
  do 
    eval $line
    ./pydatatable/groupby-pydatatable.py
  done < ./loop-groupby-data.env
  Rscript ./log-task-solution.R groupby pydatatable 1
fi

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  Rscript ./log-task-solution.R sort pydatatable 0
  while read line
  do 
    eval $line
    ./pydatatable/sort-pydatatable.py
  done < ./loop-sort-data.env
  Rscript ./log-task-solution.R sort pydatatable 1
fi

# read
if [[ "$RUN_TASKS" =~ "read" ]]; then
  Rscript ./log-task-solution.R read pydatatable 0
  while read line
  do 
    eval $line
    ./pydatatable/read-pydatatable.py
  done < ./loop-read-data.env
  Rscript ./log-task-solution.R read pydatatable 1
fi

