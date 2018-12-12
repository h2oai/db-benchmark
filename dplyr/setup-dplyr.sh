#!/bin/bash
set -e

# install R, if not already installed by setup data.table

# setup ~/.R/Makevars if not already set by setup data.table

# install devtools, a dependency to install devel dplyr
Rscript -e 'install.packages("devtools", repos="https://cloud.r-project.org", method="curl")'

# install devel dplyr
Rscript -e 'devtools::install_github("tidyverse/dplyr", method="curl")'
