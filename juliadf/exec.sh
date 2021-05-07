#!/bin/bash
set -e

if [ "$#" -ne 1 ]; then
  echo 'usage: ./juliadf/exec.sh groupby';
  exit 1
fi;

# execute benchmark script
julia -t 20 ./juliadf/$1-juliadf.jl
