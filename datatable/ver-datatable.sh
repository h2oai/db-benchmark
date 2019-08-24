#!/bin/bash
set -e

Rscript -e 'v=read.dcf(system.file(package="data.table", "DESCRIPTION"), fields=c("Version","Revision")); invisible(mapply(function(f, v) writeLines(v, file.path("datatable", f)), toupper(colnames(v)), c(v)))'
