#!/usr/bin/env Rscript

cat("# clickhouse-parse-log.R\n")

source("helpers.R")
args = commandArgs(TRUE)
stopifnot(length(args)==1L)
task = args[1L]

library(data.table)
f = list.files("clickhouse", sprintf("^log_%s_q.*\\.csv$", task), full.names=TRUE)
if (!length(f)) stop("no log files produced, did you run clickhouse sql script that will output such to clickhouse/log_[task]_q[i].csv")
d = rbindlist(lapply(f, fread, na.strings="\\N"))
stopifnot(nrow(d)==length(f)) # there should be no 0 rows files
d[,
  write.log(run=as.integer(run), task=as.character(task), data=as.character(data_name), in_rows=as.numeric(in_rows), question=as.character(question), 
            out_rows=as.numeric(out_rows), out_cols=as.integer(out_cols), solution=as.character(solution), version=as.character(version), git=as.character(git), fun=as.character(fun), 
            time_sec=as.numeric(time_sec), mem_gb=as.numeric(mem_gb), cache=as.logical(cache), chk=as.character(chk), chk_time_sec=as.numeric(chk_time_sec)),
  by = seq_len(nrow(d))] -> nul

if (!interactive()) q("no")
