#!/bin/bash
set -e

# upgrade to latest devel
Rscript -e 'install.packages("data.table", type="source", repos="http://Rdatatable.github.io/data.table", method="curl", quiet=TRUE)'

# print version
Rscript -e 'cat(sprintf("# R data.table package has been upgraded to %s (%s)\n", read.dcf(system.file("DESCRIPTION", package="data.table"), fields="Commit")[1L], packageVersion("data.table")))'
