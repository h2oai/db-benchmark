#!/bin/bash
set -e

if [ "$#" -ne 1 ]; then
  echo 'usage: ./h2o/exec.sh groupby';
  exit 1
fi;

source ./h2o/h2o.sh

h2o_active && echo 'h2o instance should not be already running, investigate' >&2
h2o_active && exit 1

# start h2o
h2o_start "h2o_$1_""$SRC_DATANAME"

# confirm h2o working
h2o_active || sleep 30
h2o_active || echo 'h2o instance should be already running, investigate' >&2
h2o_active || exit 1

# execute benchmark script
./h2o/$1-h2o.R || echo "# h2o/exec.sh: benchmark script for $SRC_DATANAME terminated with error" >&2

# stop h2o instance
h2o_stop && echo '# h2o/exec.sh: stopping h2o instance finished' || echo '# h2o/exec.sh: stopping h2o instance failed' >&2
h2o_active || exit 1
