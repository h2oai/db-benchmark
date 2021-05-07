#!/bin/bash
set -e

Rscript -e 'v=read.dcf(system.file(package="duckdb", lib.loc="./duckdb/r-duckdb", "DESCRIPTION"), fields=c("Version","Revision")); if (is.na(v[,"Revision"])) { suppressPackageStartupMessages({ requireNamespace("DBI", lib.loc="./duckdb/r-duckdb"); requireNamespace("duckdb", lib.loc="./duckdb/r-duckdb") }); v[,"Revision"] = DBI::dbGetQuery(DBI::dbConnect(duckdb::duckdb()), "SELECT source_id FROM pragma_version()")[[1L]] }; cnafill=function(x) {x=c(x); x[is.na(x)]=""; x}; fw=function(f, v) writeLines(v, file.path("duckdb", f)); invisible(mapply(fw, toupper(colnames(v)), cnafill(v)))'
