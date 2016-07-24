#!/bin/bash
set -e

# init
./h2o/init-h2o.sh

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then ./h2o/join-h2o.R; fi;

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then ./h2o/groupby-h2o.R; fi;

# shutdown
./h2o/shutdown-h2o.sh
