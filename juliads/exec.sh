#!/bin/bash
set -e

if [ "$#" -ne 1 ]; then
  echo 'usage: ./juliads/exec.sh groupby';
  exit 1
fi;

source ./path.env

# execute benchmark script
julia -t 20 ./juliads/$1-juliads.jl
