#!/bin/bash
set -e

# install R, if not already installed by setup data.table

# setup ~/.R/Makevars if not already set by setup data.table

# install stable dplyr
mkdir -p ./dplyr/r-dplyr
Rscript -e 'install.packages("dplyr", lib="./dplyr/r-dplyr")'
