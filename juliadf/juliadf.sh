#!/bin/bash
set -e

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do
    eval $line
    Rscript ./log-task-solution-data.R groupby juliadf $SRC_GRP_LOCAL 0
    ./juliadf/groupby-juliadf.jl || true
    Rscript ./log-task-solution-data.R groupby juliadf $SRC_GRP_LOCAL 1
  done < ./loop-groupby-data.env
fi
