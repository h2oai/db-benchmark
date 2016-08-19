#!/bin/bash
set -e

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  while read line
  do 
    eval $line
    ./dplyr/join-dplyr.R
  done < ./loop-join-data.env
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do
    eval $line
    ./dplyr/groupby-dplyr.R
  done < ./loop-groupby-data.env
fi

# sort
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  while read line
  do 
    eval $line
    ./dplyr/sort-dplyr.R
  done < ./loop-sort-data.env
fi
