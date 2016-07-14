#!/bin/bash
set -e

## Requirements:
# 1. run one-time setup-impala.sh to copy impala-shell binaries to mr-0xd6

# mockup
$HOME/impala/bin/impala-shell -i mr-0xd2-precise1 -B -f join-impala.sql
