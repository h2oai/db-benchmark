#!/bin/bash
set -e

# upgrade
./datatable/init-datatable.sh

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

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  while read line
  do 
    eval $line
    ./datatable/sort-datatable.R
  done < ./loop-sort-data.env
fi
