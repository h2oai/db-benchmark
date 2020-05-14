#!/bin/bash
set -e

Rscript -e 'v=read.dcf(system.file(package="h2o", lib.loc="./h2o/r-h2o", "DESCRIPTION"), fields=c("Version","Revision")); cnafill=function(x) {x=c(x); x[is.na(x)]=""; x}; fw=function(f, v) writeLines(v, file.path("h2o", f)); invisible(mapply(fw, toupper(colnames(v)), cnafill(v)))'
