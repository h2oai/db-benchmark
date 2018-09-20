#!/bin/bash
set -e

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  while read line
  do
    eval $line
    ./julia/groupby-julia.jl
  done < ./loop-groupby-data.env
fi
