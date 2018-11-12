#!/bin/bash
set -e

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R join datatable "$SRC_X"_"$SRC_Y" 0
    ./datatable/join-datatable.R
    Rscript ./log-task-solution-data.R join datatable "$SRC_X"_"$SRC_Y" 1
  done < ./loop-join-data.env
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R groupby datatable $SRC_GRP_LOCAL 0
    ./datatable/groupby-datatable.R
    Rscript ./log-task-solution-data.R groupby datatable $SRC_GRP_LOCAL 1
  done < ./loop-groupby-data.env
fi

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R sort datatable $SRC_X 0
    ./datatable/sort-datatable.R
    Rscript ./log-task-solution-data.R sort datatable $SRC_X 1
  done < ./loop-sort-data.env
fi

# read
if [[ "$RUN_TASKS" =~ "read" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R read datatable $SRC_GRP_LOCAL 0
    ./datatable/read-datatable.R
    Rscript ./log-task-solution-data.R read datatable $SRC_GRP_LOCAL 1
  done < ./loop-read-data.env
fi
