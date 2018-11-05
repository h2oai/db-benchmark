#!/bin/bash
set -e

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  Rscript ./log-task-solution.R groupby juliadf 0
  while read line
  do
    eval $line
    ./juliadf/groupby-juliadf.jl || true
  done < ./loop-groupby-data.env
  Rscript ./log-task-solution.R groupby juliadf 1
fi
