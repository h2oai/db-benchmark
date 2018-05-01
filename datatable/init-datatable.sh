#!/bin/bash
set -e

# upgrade to latest devel
Rscript -e 'data.table::update.dev.pkg(quiet=TRUE, method="curl")'
