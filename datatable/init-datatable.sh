#!/bin/bash
set -e

# upgrade to latest devel
echo 'upgrading data.table...'
Rscript -e 'data.table::update.dev.pkg(quiet=TRUE, method="curl"); v=read.dcf(system.file(package="data.table", "DESCRIPTION"), fields=c("Version","Revision")); invisible(mapply(function(f, v) writeLines(v, file.path("datatable", f)), toupper(colnames(v)), c(v)))'
