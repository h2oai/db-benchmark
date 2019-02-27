#!/usr/bin/env Rscript

cat("# clickhouse-parse-log.R\n")

source("helpers.R")
args = commandArgs(TRUE)
stopifnot(length(args)==2L)
task = args[1L]
data_name = args[2L]

library(data.table)
f = list.files("clickhouse", sprintf("^log_%s_%s_q.*\\.csv$", task, data_name), full.names=TRUE)
if (!length(f)) stop("no log files produced, did you run clickhouse sql script that will output such to clickhouse/log_[task]_[data_name]_q[i]_r[j].csv")
d = rbindlist(lapply(f, fread, na.strings="\\N"))
stopifnot(nrow(d)==length(f), # there should be no 0 rows files
          all(d$task==task),
          all(d$data_name==data_name))
d[,
  write.log(run=as.integer(run), timestamp=as.numeric(timestamp), task=as.character(task), data=as.character(data_name), in_rows=as.numeric(in_rows), question=as.character(question), 
            out_rows=as.numeric(out_rows), out_cols=as.integer(out_cols), solution=as.character(solution), version=as.character(version), git=as.character(git), fun=as.character(fun), 
            time_sec=as.numeric(time_sec), mem_gb=as.numeric(mem_gb), cache=as.logical(cache), chk=as.character(chk), chk_time_sec=as.numeric(chk_time_sec)),
  by = seq_len(nrow(d))] -> nul

if (!interactive()) {
  unlink(f)
  q("no")
}
