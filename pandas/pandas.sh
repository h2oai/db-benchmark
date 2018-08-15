#!/bin/bash
set -e

source ./pandas/py-pandas/bin/activate

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  while read line
  do 
    eval $line
    ./pandas/join-pandas.py
  done < ./loop-join-data.env
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do 
    eval $line
    ./pandas/groupby-pandas.py || true
  done < ./loop-groupby-data.env
fi

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  while read line
  do 
    eval $line
    ./pandas/sort-pandas.py
  done < ./loop-sort-data.env
fi

# read
if [[ "$RUN_TASKS" =~ "read" ]]; then
  while read line
  do 
    eval $line
    ./pandas/read-pandas.py
  done < ./loop-read-data.env
fi

