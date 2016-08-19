#!/bin/bash
set -e

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
    ./pandas/groupby-pandas.py
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
