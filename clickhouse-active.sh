#!/bin/bash
set -e

bash -c 'clickhouse-client --query="SELECT 0;"; exit $?' > /dev/null 2>&1
