#!/bin/bash
set -e

# upgrade to latest released
echo 'upgrading clickhouse-server clickhouse-client...'
apt-get install --only-upgrade clickhouse-server clickhouse-client
