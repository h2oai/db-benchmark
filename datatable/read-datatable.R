#!/usr/bin/env Rscript

cat("# read-datatable.R\n")

source("./helpers.R")
source("./datatable/helpers-datatable.R")

suppressPackageStartupMessages(library(data.table))
ver = packageVersion("data.table")
git = datatable.git()
task = "read"
solution = "data.table"
fun = "fread"
cache = TRUE

src_grp = Sys.getenv("SRC_GRP")
data_name = basename(src_grp)
options("datatable.showProgress"=FALSE)

cat("reading...\n")

question = "all rows" #1
t = system.time(print(dim(ans<-fread(data_name))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v3))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-fread(data_name))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v3))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-fread(data_name))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v3))])[["elapsed"]]
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

question = "top 100 rows" #2
t = system.time(print(dim(ans<-fread(data_name, nrows=100))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v3))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-fread(data_name, nrows=100))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v3))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-fread(data_name, nrows=100))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v3))])[["elapsed"]]
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

if( !interactive() ) q("no", status=0)
