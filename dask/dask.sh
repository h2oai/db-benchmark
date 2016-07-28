#!/bin/bash
set -e

# init
./dask/init-dask.sh

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  while read line
  do 
    eval $line
    ./dask/join-dask.py
  done < ./loop-join-data.env
fi

# # groupby
# if [[ "$RUN_TASKS" =~ "groupby" ]]; then
#   while read line
#   do 
#     eval $line
#     ./dask/groupby-dask.py
#   done < ./loop-groupby-data.env
# fi

# shutdown
./dask/shutdown-dask.sh
