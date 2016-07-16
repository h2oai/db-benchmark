#!/bin/bash
set -e

## Requirements:
# 1. exported vars SRC_X and SRC_Y, see join.sh

export HOSTS="mr-0xd1 mr-0xd2 mr-0xd3 mr-0xd4 mr-0xd5 mr-0xd7 mr-0xd8 mr-0xd9 mr-0xd10"
export MEM="-Xmx220G -Xms220G"
export H2O_HOST="mr-0xd8"
export H2O_PORT=55888
export H2O_NAME=$USER"H2O"

# kcl: always kill all hosts to not leave lying around when changing configs
for i in $HOSTS; do (ssh $USER@$i "killall -9 java 2>&1 > /dev/null" &) 2>&1 > /dev/null; done && sleep 5

for i in $HOSTS; do (ssh $USER@$i "nohup java $MEM -cp h2o.jar water.H2OApp -name $H2O_NAME -port $H2O_PORT 2>&1 >> ~/cluster.log" & ); done && sleep 2 && echo Started cluster ok on $HOSTS
sleep 15

./join-h2o.R

# kcl, shutdown
for i in $HOSTS; do (ssh $USER@$i "killall -9 java 2>&1 > /dev/null" &) 2>&1 > /dev/null; done && sleep 5
