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
    ./pydatatable/groupby-pydatatable.py
  done < ./loop-groupby-data.env
fi

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  while read line
  do 
    eval $line
    ./pydatatable/sort-pydatatable.py
  done < ./loop-sort-data.env
fi

# read
if [[ "$RUN_TASKS" =~ "read" ]]; then
  while read line
  do 
    eval $line
    ./pydatatable/read-pydatatable.py
  done < ./loop-read-data.env
fi

