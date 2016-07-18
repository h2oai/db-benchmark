#!/bin/bash
set -e

for i in $CLUSTER; do (ssh $USER@$i "killall -9 java 2>&1 > /dev/null" &) 2>&1 > /dev/null; done && sleep 5
