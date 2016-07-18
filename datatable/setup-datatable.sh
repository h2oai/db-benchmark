#!/bin/bash
set -e

# Required only on client machine

# install R on client machine, then from shell install data.table
Rscript -e 'install.packages("data.table", type="source", repos="https://Rdatatable.github.io/data.table", method="curl")'
