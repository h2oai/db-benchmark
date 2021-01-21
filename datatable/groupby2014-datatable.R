#!/usr/bin/env Rscript

cat("# groupby2014-datatable.R\n")

source("./_helpers/helpers.R")

stopifnot(requireNamespace(c("bit64"), quietly=TRUE)) # used in chk to sum numeric columns
suppressPackageStartupMessages(library("data.table", lib.loc="./datatable/r-datatable"))
setDTthreads(0L)
ver = packageVersion("data.table")
git = data.table:::.git(quiet=TRUE)
task = "groupby2014"
solution = "data.table"
fun = "[.data.table"
cache = TRUE
on_disk = FALSE

data_name = Sys.getenv("SRC_DATANAME")
src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

x = fread(src_grp, showProgress=FALSE)
print(nrow(x))

task_init = proc.time()[["elapsed"]]
cat("grouping...\n")

question = "sum v1 by id1" # q1
t = system.time(print(dim(ans<-x[, sum(v1), keyby=id1])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(V1)))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, sum(v1), keyby=id1])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(V1)))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 by id1:id2" # q2
t = system.time(print(dim(ans<-x[, sum(v1), keyby='id1,id2'])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(V1)))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, sum(v1), keyby='id1,id2'])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(V1)))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 mean v3 by id3" # q3
t = system.time(print(dim(ans<-x[, list(sum(v1), mean(v3)), keyby=id3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(V1)), sum(V2))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, list(sum(v1), mean(v3)), keyby=id3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(V1)), sum(V2))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "mean v1:v3 by id4" # q4
t = system.time(print(dim(ans<-x[, lapply(.SD, mean), keyby=id4, .SDcols=7:9])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(v2), sum(v3))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, lapply(.SD, mean), keyby=id4, .SDcols=7:9])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(v2), sum(v3))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1:v3 by id6" # q5
t = system.time(print(dim(ans<-x[, lapply(.SD, sum), keyby=id6, .SDcols=7:9])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)), sum(bit64::as.integer64(v2)), sum(v3))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, lapply(.SD, sum), keyby=id6, .SDcols=7:9])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)), sum(bit64::as.integer64(v2)), sum(v3))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

cat(sprintf("grouping finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
