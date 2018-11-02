#!/bin/bash
set -e

source ./pandas/py-pandas/bin/activate

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  Rscript ./log-task-solution.R join pandas 0
  while read line
  do 
    eval $line
    ./pandas/join-pandas.py
  done < ./loop-join-data.env
  Rscript ./log-task-solution.R join pandas 1
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  Rscript ./log-task-solution.R groupby pandas 0
  while read line
  do 
    eval $line
    ./pandas/groupby-pandas.py || true
  done < ./loop-groupby-data.env
  Rscript ./log-task-solution.R groupby pandas 1
fi

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  Rscript ./log-task-solution.R sort pandas 0
  while read line
  do 
    eval $line
    ./pandas/sort-pandas.py
  done < ./loop-sort-data.env
  Rscript ./log-task-solution.R sort pandas 1
fi

# read
if [[ "$RUN_TASKS" =~ "read" ]]; then
  Rscript ./log-task-solution.R read pandas 0
  while read line
  do 
    eval $line
    ./pandas/read-pandas.py
  done < ./loop-read-data.env
  Rscript ./log-task-solution.R read pandas 1
fi

