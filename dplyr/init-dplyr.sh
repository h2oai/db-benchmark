#!/bin/bash
set -e

# upgrade to latest devel
echo 'upgrading dplyr...'
Rscript -e 'devtools::install_github("tidyverse/dplyr", quiet=TRUE, method="curl")'
