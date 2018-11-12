#!/bin/bash
set -e

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R join dplyr "$SRC_X"_"$SRC_Y" 0
    ./dplyr/join-dplyr.R
    Rscript ./log-task-solution-data.R join dplyr "$SRC_X"_"$SRC_Y" 1
  done < ./loop-join-data.env
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do
    eval $line
    Rscript ./log-task-solution-data.R groupby dplyr $SRC_GRP_LOCAL 0
    ./dplyr/groupby-dplyr.R || true
    Rscript ./log-task-solution-data.R groupby dplyr $SRC_GRP_LOCAL 1
  done < ./loop-groupby-data.env
fi

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R sort dplyr $SRC_X 0
    ./dplyr/sort-dplyr.R
    Rscript ./log-task-solution-data.R sort dplyr $SRC_X 1
  done < ./loop-sort-data.env
fi

# read
if [[ "$RUN_TASKS" =~ "read" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R read dplyr $SRC_GRP_LOCAL 0
    ./dplyr/read-dplyr.R
    Rscript ./log-task-solution-data.R read dplyr $SRC_GRP_LOCAL 1
  done < ./loop-read-data.env
fi
