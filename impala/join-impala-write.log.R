#!/usr/bin/env Rscript

source("./helpers.R")

v = commandArgs(TRUE)
if(!length(v)) stop("join-impala-write.log.R must be called with command line argument, a filename of impala SQL log.")
if(!file.exists(v[1L])) stop("join-impala-write.log.R file of impala SQL log file does not exists")
log = readLines(v[1L])

## CREATE TABLE AS SELECT ...
# ilog = grep("Query: create TABLE r STORED AS PARQUET AS SELECT", fixed=TRUE, log)
# l = lapply(ilog, function(i) log[i+0:8])
# lapply(l, function(x) {
#   stopifnot(grepl("Query: select UNIX_TIMESTAMP()", x[7L], fixed=TRUE), grepl("Query: create TABLE r STORED AS PARQUET AS SELECT", x[1L], fixed=TRUE))
#   t = sapply(strsplit(c(x[3L], x[6L]), split=" row(s) in ", fixed=TRUE), `[`, 2L)
#   t = as.numeric(substr(t, 1, nchar(t)-1L))
#   x = strsplit(x[9L], ",", fixed=TRUE)[[1L]]
#   write.log(timestamp=as.numeric(x[1L]), task=x[2L], data=x[3L], in_rows=as.integer(x[4L]), out_rows=as.integer(x[5L]), solution=x[6L], fun=x[7L], run=as.integer(x[8L]), time_sec=as.numeric(sum(t)), mem_gb=NA_real_)
# }) -> nul

## SELECT COUNT(*) FROM (SELECT ...)
## COMPUTE STATS + SELECT COUNT(*) FROM (SELECT ...)
ilog = grep("Query: select COUNT(*) FROM (SELECT", fixed=TRUE, log)
l = lapply(ilog, function(i) log[i+0:5])
lapply(l, function(x) {
  stopifnot(grepl("Query: select UNIX_TIMESTAMP()", x[4L], fixed=TRUE), grepl("Query: select COUNT(*) FROM (SELECT", x[1L], fixed=TRUE))
  t = strsplit(x[3L], split=" row(s) in ", fixed=TRUE)[[1L]][2L]
  if (substr(t, nchar(t), nchar(t))!="s" && grepl("[0-9]", substr(t, nchar(t)-1L, nchar(t)-1L))) stop(sprintf("Failed to parse timing of impala query, expected numeric with traling 's' like 0.1s, got %s", t))
  t = as.numeric(substr(t, 1, nchar(t)-1L))
  out_rows = as.integer(x[2L])
  x = strsplit(x[6L], ",", fixed=TRUE)[[1L]]
  write.log(timestamp=as.numeric(x[1L]), task=x[2L], data=x[3L], in_rows=as.integer(x[4L]), out_rows=out_rows, solution=x[6L], fun=x[7L], run=as.integer(x[8L]), time_sec=as.numeric(t), mem_gb=NA_real_)
}) -> nul

if( !interactive() ) q("no", status=0)
