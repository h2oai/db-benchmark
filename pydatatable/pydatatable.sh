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
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R groupby pydatatable $SRC_GRP_LOCAL 0
    ./pydatatable/groupby-pydatatable.py
    Rscript ./log-task-solution-data.R groupby pydatatable $SRC_GRP_LOCAL 1
  done < ./loop-groupby-data.env
fi

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R sort pydatatable $SRC_X 0
    ./pydatatable/sort-pydatatable.py
    Rscript ./log-task-solution-data.R sort pydatatable $SRC_X 1
  done < ./loop-sort-data.env
fi

# read
if [[ "$RUN_TASKS" =~ "read" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R read pydatatable $SRC_GRP_LOCAL 0
    ./pydatatable/read-pydatatable.py
    Rscript ./log-task-solution-data.R read pydatatable $SRC_GRP_LOCAL 1
  done < ./loop-read-data.env
fi

