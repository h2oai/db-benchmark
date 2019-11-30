#!/usr/bin/env Rscript

cat("# clickhouse-parse-log.R: starting to parse timings from clickhouse/log/.\n")

source("./_helpers/helpers.R")
args = commandArgs(TRUE) # args = c("groupby","G1_1e6_1e2_0_0")
stopifnot(length(args)==2L)
task = args[1L]
data_name = args[2L]

library(data.table)

fcsv = list.files("clickhouse/log", sprintf("^%s_%s_q.*\\.csv$", task, data_name), full.names=TRUE)
if (!length(fcsv)) stop("no log files produced, did you run clickhouse sql script that will output such to clickhouse/log/[task]_[data_name]_q[i]_r[j].csv")
d = rbindlist(lapply(fcsv, fread, na.strings="\\N")) # fill=TRUE for debugging type column in some queries
if (!nrow(d)) stop("timing log files empty")
stopifnot(all(d$task==task), all(d$data_name==data_name))
d[,
  write.log(run=as.integer(run), timestamp=as.numeric(timestamp), task=as.character(task), data=as.character(data_name), in_rows=get.nrow(data_name=data_name), question=as.character(question), 
            out_rows=as.numeric(NA), out_cols=as.integer(NA), solution=as.character(solution), version=as.character(version), git=as.character(NA), fun=as.character(fun), 
            time_sec=as.numeric(time_sec), mem_gb=as.numeric(NA), cache=as.logical(cache), chk=as.character(NA), chk_time_sec=as.numeric(NA)),
  by = seq_len(nrow(d))] -> nul

cat("# clickhouse-parse-log.R: parsing timings to time.csv finished\n")

if (!interactive()) q("no")
