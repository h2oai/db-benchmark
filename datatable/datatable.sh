#!/bin/bash
set -e

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  Rscript ./log-task-solution.R join datatable 0
  while read line
  do 
    eval $line
    ./datatable/join-datatable.R
  done < ./loop-join-data.env
  Rscript ./log-task-solution.R join datatable 1
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  Rscript ./log-task-solution.R groupby datatable 0
  while read line
  do 
    eval $line
    ./datatable/groupby-datatable.R
  done < ./loop-groupby-data.env
  Rscript ./log-task-solution.R groupby datatable 1
fi

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  Rscript ./log-task-solution.R sort datatable 0
  while read line
  do 
    eval $line
    ./datatable/sort-datatable.R
  done < ./loop-sort-data.env
  Rscript ./log-task-solution.R sort datatable 1
fi

# read
if [[ "$RUN_TASKS" =~ "read" ]]; then
  Rscript ./log-task-solution.R read datatable 0
  while read line
  do 
    eval $line
    ./datatable/read-datatable.R
  done < ./loop-read-data.env
  Rscript ./log-task-solution.R read datatable 1
fi
