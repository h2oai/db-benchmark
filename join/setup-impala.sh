#!/bin/bash
set -e

## Requirements:
# 1. passwordless ssh to mr-0xd2-precise1, or just run below rsync commands in shell

mkdir -p $HOME/impala/bin $HOME/impala/lib
rsync -e ssh $USER@mr-0xd2-precise1:/opt/cloudera/parcels/CDH-5.7.1-1.cdh5.7.1.p0.11/bin/impala-shell $HOME/impala/bin/impala-shell
rsync -r -e ssh $USER@mr-0xd2-precise1:/opt/cloudera/parcels/CDH-5.7.1-1.cdh5.7.1.p0.11/lib/impala-shell/ $HOME/impala/lib/impala-shell

# test client
$HOME/impala/bin/impala-shell -i mr-0xd2-precise1 -q "select version();"
