#!/usr/bin/env Rscript

cat("# clickhouse-parse-log.R: starting to parse timings from clickhouse/log/.\n")

source("./_helpers/helpers.R")
args = commandArgs(TRUE) # args = c("groupby","G1_1e6_1e2_0_0")
stopifnot(length(args)==2L)
task = args[1L]
data_name = args[2L]

library(data.table)
# sort files according to question and run
sort_q_r = function(f) {
  tmp = strsplit(tools::file_path_sans_ext(basename(f)), "_", fixed=TRUE)
  if (length(len<-unique(lengths(tmp)))!=1L)
    stop("files names for some of logs differs in number of underscores, it should be clickhouse/log/[task]_[data_name]_q[i]_r[j].csv")
  stopifnot(len>1L)
  qr = rbindlist(lapply(lapply(tmp, `[`, c(len-1L,len)), function(x) {
    stopifnot(substr(x[1L], 1L, 1L)=="q", substr(x[2L], 1L, 1L)=="r")
    list(q=as.integer(substr(x[1L], 2L, nchar(x[1L]))), r=as.integer(substr(x[2L], 2L, nchar(x[2L]))))
  }))
  o = data.table:::forderv(qr) ## https://github.com/Rdatatable/data.table/issues/3447
  if (!length(o)) f else f[o]
}
fcsv = list.files("clickhouse/log", sprintf("^%s_%s_q.*\\.csv$", task, data_name), full.names=TRUE)
if (!length(fcsv))
  stop("no log files produced, did you run clickhouse sql script that will output such to clickhouse/log/[task]_[data_name]_q[i]_r[j].csv")
fcsv = sort_q_r(fcsv)
d = rbindlist(lapply(fcsv, fread, na.strings="\\N")) # fill=TRUE for debugging type column in some queries
if (!nrow(d))
  stop("timing log files empty")
stopifnot(all(d$task==task), all(d$data_name==data_name))
.in_rows = strsplit(data_name, "_", fixed=TRUE)[[1L]][[2L]] ## taken from data_name because for join CH will sum in rows from both tables
d[,
  write.log(run=as.integer(run), timestamp=as.numeric(timestamp), task=as.character(task), data=as.character(data_name), in_rows=as.numeric(.in_rows), question=as.character(question),
            out_rows=as.numeric(NA), out_cols=as.integer(NA), solution=as.character(solution), version=as.character(version), git=as.character(NA), fun=as.character(fun), 
            time_sec=as.numeric(time_sec), mem_gb=as.numeric(NA), cache=as.logical(cache), chk=as.character(NA), chk_time_sec=as.numeric(NA), on_disk=as.logical(on_disk)),
  by = seq_len(nrow(d))] -> nul

cat("# clickhouse-parse-log.R: parsing timings to time.csv finished\n")

if (!interactive()) q("no")
