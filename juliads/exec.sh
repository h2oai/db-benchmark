#!/bin/bash
set -e

if [ "$#" -ne 1 ]; then
  echo 'usage: ./juliads/exec.sh groupby';
  exit 1
fi;

source ./path.env

# execute benchmark script
julia -t $(lscpu -p | egrep -v '^#' | sort -u -t, -k 2,4 | wc -l) ./juliads/$1-juliads.jl
