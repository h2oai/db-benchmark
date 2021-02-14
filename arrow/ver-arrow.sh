#!/bin/bash
set -e

Rscript -e 'v=read.dcf(system.file(package="arrow", lib.loc="./arrow/r-arrow", "DESCRIPTION"), fields=c("Version","RemoteSha")); colnames(v)[colnames(v)=="RemoteSha"]="Revision"; cnafill=function(x) {x=c(x); x[is.na(x)]=""; x}; fw=function(f, v) writeLines(v, file.path("arrow", f)); invisible(mapply(fw, toupper(colnames(v)), cnafill(v)))'
