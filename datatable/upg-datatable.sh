#!/bin/bash
set -e

# upgrade to latest devel
echo 'upgrading data.table...'
# Rscript -e 'data.table::update.dev.pkg(quiet=TRUE, method="curl", lib="./datatable/r-datatable")'
Rscript -e 'update.packages(lib.loc = "./datatable/r-datatable", repos="https://rdatatable.gitlab.io/data.table", method="curl")'

# Rscript -e 'new.packages(lib.loc = "./datatable/r-datatable", epos="https://rdatatable.gitlab.io/data.table",
#              instPkgs = installed.packages(lib.loc = lib.loc, …),
#              method, available = NULL, ask = FALSE, …,
#              type = getOption("pkgType"))