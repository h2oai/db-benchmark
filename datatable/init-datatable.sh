#!/bin/bash
set -e

# upgrade to latest devel only if hash not equal
Rscript -e 'source("helpers.R"); install.dev.package("data.table", repo="http://Rdatatable.github.io/data.table", field="Commit", quiet=TRUE, method="curl")'
