#!/bin/bash
set -e

Rscript -e 'v=read.dcf(system.file(package="data.table", lib.loc="./datatable/r-datatable", "DESCRIPTION"), fields=c("Version","Revision")); cnafill=function(x) {x=c(x); x[is.na(x)]=""; x}; fw=function(f, v) writeLines(v, file.path("datatable", f)); invisible(mapply(fw, toupper(colnames(v)), cnafill(v)))'
