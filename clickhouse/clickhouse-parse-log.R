#!/usr/bin/env Rscript

cat("# clickhouse-parse-log.R\n")

source("helpers.R")
args = commandArgs(TRUE) # args = c("groupby","G1_1e6_1e2_0_0")
stopifnot(length(args)==2L)
task = args[1L]
data_name = args[2L]

library(data.table)

fout = paste(file.path("clickhouse","log",paste(task, data_name, sep="_")), "out", sep=".")
if (!file.exists(fout)) stop("no out file produced, did you run clickhouse sql script that will output timings to STDERR that should be redirected to to clickhouse/log/[task]_[data_name].out")
out = readLines(fout)
if (!identical(sprintf("%.3f", as.numeric(out)), out)) stop("some timings recorded by clickhouse in STDERR does not seems to be number, probably some errors strings, investigate")
if (length(out) < 2L) stop("out file has only single line (time of technical select timestamp query), that means the very first benchmark query killed server, investigate")
fcsv = list.files("clickhouse/log", sprintf("^%s_%s_q.*\\.csv$", task, data_name), full.names=TRUE)
if (!length(fcsv)) stop("no log files produced, did you run clickhouse sql script that will output such to clickhouse/log/[task]_[data_name]_q[i]_r[j].csv")
d = rbindlist(lapply(fcsv, fread, na.strings="\\N")) # fill=TRUE for debugging type column in some queries
stopifnot(all(d$task==task), all(d$data_name==data_name))
if (nrow(d)*2L != length(out)) {
  if (nrow(d)*2L == length(out)+1L) { # last query got killed
    out = c(out, NA)
  } else stop("number of timings measured from STDERR by clickhouse does not match to timings recorded from script")
}
time_i = seq.int(2L, length(out), by=2L)
d[, "time_sec" := as.numeric(out[time_i])]
d[,
  write.log(run=as.integer(run), timestamp=as.numeric(timestamp), task=as.character(task), data=as.character(data_name), in_rows=get.nrow(data_name=data_name), question=as.character(question), 
            out_rows=as.numeric(NA), out_cols=as.integer(NA), solution=as.character(solution), version=as.character(version), git=as.character(NA), fun=as.character(fun), 
            time_sec=as.numeric(time_sec), mem_gb=as.numeric(NA), cache=as.logical(cache), chk=as.character(NA), chk_time_sec=as.numeric(NA)),
  by = seq_len(nrow(d))] -> nul

if (!interactive()) q("no")
