#!/bin/bash
set -e

# init
./h2o/init-h2o.sh

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  while read line
  do 
    eval $line
    ./h2o/join-h2o.R
  done < ./loop-join-data.env
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do 
    eval $line
    ./h2o/groupby-h2o.R
  done < ./loop-groupby-data.env
fi

# shutdown
./h2o/shutdown-h2o.sh
