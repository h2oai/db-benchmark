#!/bin/bash
set -e

# init
./h2o/init-h2o.sh

# join
./h2o/join-h2o.R

# shutdown
./h2o/shutdown-h2o.sh
