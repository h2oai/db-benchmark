#!/usr/bin/env Rscript

source("./helpers.R")

v = commandArgs(TRUE)
if(!length(v)) stop("impala-write.log.R must be called with command line argument, a filename of impala SQL log.")
if(!file.exists(v[1L])) stop("impala-write.log.R file of impala SQL log file does not exists")
log = readLines(v[1L])

# impala version
log_entry = log[grep("impalad version", log)[1L]]
entry_end = strsplit(log_entry, "impalad version ", fixed=TRUE)[[1L]][2L]
ver = strsplit(entry_end, "-", fixed=TRUE)[[1L]][1L]

# isolate test body
ilog = grep("###impala-body", log)
log = log[seq(ilog[1L]+3L, ilog[2L]-1L)] # cut c("###impala-body", "--------", "Executed in 0.00s") and closing "###impala-body"

# catch each log entry
ilog = grep("Query: select UNIX_TIMESTAMP() _timestamp", log, fixed=TRUE)

# get expected output fields
l = lapply(ilog, function(i) log[(i-3L):(i+1L)])
# extract timing from output row
extract_time_sec = function(x) {
  t = strsplit(x, split=" row(s) in ", fixed=TRUE)[[1L]][2L]
  if (substr(t, nchar(t), nchar(t))!="s" && grepl("[0-9]", substr(t, nchar(t)-1L, nchar(t)-1L))) stop(sprintf("Failed to parse timing of impala query, expected numeric with traling 's' like 0.1s, got %s", t))
  as.numeric(substr(t, 1, nchar(t)-1L))
}
# collect all metadata and append to csv
lapply(l, function(x) {
  cat(sprintf("Processing impala log. %s\n", x[1L]))
  out_rows = bit64::as.integer64(x[2L])
  t = extract_time_sec(x[3L])
  log_row = strsplit(x[5L], ",")[[1L]]
  timestamp = as.numeric(log_row[1L])
  task = log_row[2L]
  data_name = log_row[3L]
  in_rows = bit64::as.integer64(log_row[4L])
  question = log_row[5L]
  solution = log_row[6L]
  fun = log_row[7L]
  run = as.integer(log_row[8L])
  write.log(timestamp=timestamp, task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=run, time_sec=as.numeric(t))
}) -> nul

if( !interactive() ) q("no", status=0)
