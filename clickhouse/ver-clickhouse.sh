#!/bin/bash
set -e

source ./ch.sh # clickhouse helper scripts

ch_installed && clickhouse-client --version-clean > clickhouse/VERSION && echo "" > clickhouse/REVISION
