#!/bin/bash
set -e

# upgrade to latest devel
echo 'upgrading data.table...'
Rscript -e 'data.table::update.dev.pkg(quiet=TRUE, method="curl")'
