#!/bin/bash
set -e

# upgrade to latest devel
echo 'upgrading dplyr...'
Rscript -e 'remotes::install_cran("dplyr", quiet=TRUE)'
