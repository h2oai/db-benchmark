#!/bin/bash
set -e

# Required only on client machine

# install R on client machine, then from shell install data.table
Rscript -e 'install.packages("dplyr", type="source", repos="https://cran.rstudio.com", method="curl")'
