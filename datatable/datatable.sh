#!/bin/bash
set -e

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then ./datatable/join-datatable.R; fi;

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then ./datatable/groupby-datatable.R; fi;
