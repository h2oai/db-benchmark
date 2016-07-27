#!/bin/bash
set -e

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  while read line
  do 
    eval $line
    ./datatable/join-datatable.R
  done < ./loop-join-data.env
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do 
    eval $line
    ./datatable/groupby-datatable.R
  done < ./loop-groupby-data.env
fi
