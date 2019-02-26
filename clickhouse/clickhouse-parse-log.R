#!/usr/bin/env Rscript

cat("# clickhouse-parse-log.R\n")

args = commandArgs(TRUE)
stopifnot(length(args)==1L)
task = args[1L]

library(data.table)
d = rbindlist(lapply(f<-list.files("clickhouse", sprintf("^log_%s_q.*\\.csv$", task), full.names=TRUE), fread))
stopifnot(nrow(d)==f) # there should be no 0 rows files
d[,
  write.log(run=run, task=task, data=data_name, in_rows=in_rows, question=question, 
            out_rows=out_rows, out_cols=out_cols, solution=solution, version=ver, git=git, fun=fun, 
            time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt),
  by = seq_len(nrow(d))] -> nul

if (!interactive()) {
  unlink(f)
  q("no")
}
