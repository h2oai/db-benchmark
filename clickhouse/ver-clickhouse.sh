#!/bin/bash
set -e

ch_installed && clickhouse-client --version-clean > clickhouse/VERSION && echo "" > clickhouse/REVISION
