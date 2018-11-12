#!/bin/bash
set -e

source ./pandas/py-pandas/bin/activate

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R join pandas "$SRC_X"_"$SRC_Y" 0
    ./pandas/join-pandas.py
    Rscript ./log-task-solution-data.R join pandas "$SRC_X"_"$SRC_Y" 1
  done < ./loop-join-data.env
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R groupby pandas $SRC_GRP_LOCAL 0
    ./pandas/groupby-pandas.py || true
    Rscript ./log-task-solution-data.R groupby pandas $SRC_GRP_LOCAL 1
  done < ./loop-groupby-data.env
fi

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R sort pandas $SRC_X 0
    ./pandas/sort-pandas.py
    Rscript ./log-task-solution-data.R sort pandas $SRC_X 1
  done < ./loop-sort-data.env
fi

# read
if [[ "$RUN_TASKS" =~ "read" ]]; then
  while read line
  do 
    eval $line
    Rscript ./log-task-solution-data.R read pandas $SRC_GRP_LOCAL 0
    ./pandas/read-pandas.py
    Rscript ./log-task-solution-data.R read pandas $SRC_GRP_LOCAL 1
  done < ./loop-read-data.env
fi

