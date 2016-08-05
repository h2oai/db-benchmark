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
git = sub(")", "", strsplit(entry_end, "(build ", fixed=TRUE)[[1L]][2L], fixed=TRUE)

# isolate test body
ilog = grep("impala-out-test-body", log)
if(length(ilog)!=2L) stop(sprintf("impala sql log cannot be processed, there must be exact 2 lines of 'impala-out-test-body', sql script might have not completed successfully."))
log = log[seq(ilog[1L]+3L, ilog[2L]-1L)] # cut c("impala-out-test-body", "--------", "Executed in 0.00s") and closing "impala-out-test-body"

# catch each log entry
ilog = grep("Query: select UNIX_TIMESTAMP() _timestamp", log, fixed=TRUE)

# extract timing from output row
extract_time_sec = function(x) {
  t = strsplit(x, split=" row(s) in ", fixed=TRUE)[[1L]][2L]
  if (substr(t, nchar(t), nchar(t))!="s" && grepl("[0-9]", substr(t, nchar(t)-1L, nchar(t)-1L))) stop(sprintf("Failed to parse timing of impala query, expected numeric with traling 's' like 0.1s, got %s", t))
  as.numeric(substr(t, 1, nchar(t)-1L))
}

# parse each query, handle cache and non-cache
parse_query = function(i, log) {
  # parse info
  log_row = strsplit(log[i+1L], ",")[[1L]]
  timestamp = as.numeric(log_row[1L])
  task = log_row[2L]
  data_name = log_row[3L]
  in_rows = bit64::as.integer64(log_row[4L])
  question = log_row[5L]
  #out_rows = log_row[6L] # static '' here, count from separate query results below
  solution = log_row[7L]
  fun = log_row[8L]
  run = as.integer(log_row[9L])
  cache = as.logical(log_row[10L])
  if (!cache) {
    out_rows = bit64::as.integer64(log[i-2L])
    time_sec = extract_time_sec(log[i-1L])
    chk = NA_character_
    chk_time_sec = NA_real_
  } else {
    #cat(paste((-9):1, log[(i-9L):(i+1L)]), sep="\n")
    out_rows = bit64::as.integer64(log[i-5L])
    time_sec = extract_time_sec(log[i-7L]) + extract_time_sec(log[i-4L]) # time of create table as select + time of select count
    chk = log[i-2L]
    chk_time_sec = extract_time_sec(log[i-1L])
  }
  write.log(timestamp=timestamp, task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, git=git, fun=fun, run=run, time_sec=time_sec, cache=cache, chk=chk, chk_time_sec=chk_time_sec)
}
lapply(ilog, parse_query, log) -> nul

if( !interactive() ) q("no", status=0)
