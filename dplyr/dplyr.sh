#!/bin/bash
set -e

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  Rscript ./log-task-solution.R join dplyr 0
  while read line
  do 
    eval $line
    ./dplyr/join-dplyr.R
  done < ./loop-join-data.env
  Rscript ./log-task-solution.R join dplyr 1
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  Rscript ./log-task-solution.R groupby dplyr 0
  while read line
  do
    eval $line
    ./dplyr/groupby-dplyr.R || true
  done < ./loop-groupby-data.env
  Rscript ./log-task-solution.R groupby dplyr 1
fi

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  Rscript ./log-task-solution.R sort dplyr 0
  while read line
  do 
    eval $line
    ./dplyr/sort-dplyr.R
  done < ./loop-sort-data.env
  Rscript ./log-task-solution.R sort dplyr 1
fi

# read
if [[ "$RUN_TASKS" =~ "read" ]]; then
  Rscript ./log-task-solution.R read dplyr 0
  while read line
  do 
    eval $line
    ./dplyr/read-dplyr.R
  done < ./loop-read-data.env
  Rscript ./log-task-solution.R read dplyr 1
fi
