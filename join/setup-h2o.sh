#!/bin/bash
set -e

## Requirements:
# 1. on mr-0xd6 you should have ~/h2o.jar, file originally from h2o-hadoop/h2o-hdp2.2-assembly/build/libs/h2odriver.jar

export HOSTS="mr-0xd1 mr-0xd2 mr-0xd3 mr-0xd4 mr-0xd5 mr-0xd7 mr-0xd8 mr-0xd9 mr-0xd10"

# copy h2o.jar to nodes
for i in $HOSTS; do cmd="rsync -aq $HOME/h2o.jar $USER@$i:h2o.jar"; echo $cmd; eval $cmd; done
