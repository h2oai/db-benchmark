#!/bin/bash
set -e

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then ./pandas/join-pandas.py; fi;

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then ./pandas/groupby-pandas.py; fi;
