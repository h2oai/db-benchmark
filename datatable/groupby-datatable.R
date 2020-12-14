#!/usr/bin/env Rscript

cat("# groupby-datatable.R\n")

source("./_helpers/helpers.R")

stopifnot(requireNamespace(c("bit64"), quietly=TRUE)) # used in chk to sum numeric columns
suppressPackageStartupMessages(library("data.table", lib.loc="./datatable/r-datatable"))
setDTthreads(0L)
ver = packageVersion("data.table")
git = data.table:::.git(quiet=TRUE)
task = "groupby"
solution = "data.table"
fun = "[.data.table"
cache = TRUE
on_disk = FALSE

data_name = Sys.getenv("SRC_DATANAME")
src_grp = file.path("data", paste(data_name, "csv", sep="."))
#src_grp = file.path("data", paste(data_name, "rds", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

x = fread(src_grp, showProgress=FALSE, stringsAsFactors=TRUE, na.strings="")
#x = readRDS(src_grp)
#setDT(x)
print(nrow(x))

task_init = proc.time()[["elapsed"]]
cat("grouping...\n")

question = "sum v1 by id1" # q1
t = system.time(print(dim(ans<-x[, .(v1=sum(v1, na.rm=TRUE)), by=id1])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, .(v1=sum(v1, na.rm=TRUE)), by=id1])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 by id1:id2" # q2
t = system.time(print(dim(ans<-x[, .(v1=sum(v1, na.rm=TRUE)), by=.(id1, id2)])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, .(v1=sum(v1, na.rm=TRUE)), by=.(id1, id2)])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 mean v3 by id3" # q3
t = system.time(print(dim(ans<-x[, .(v1=sum(v1, na.rm=TRUE), v3=mean(v3, na.rm=TRUE)), by=id3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)), sum(v3))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, .(v1=sum(v1, na.rm=TRUE), v3=mean(v3, na.rm=TRUE)), by=id3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)), sum(v3))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "mean v1:v3 by id4" # q4
t = system.time(print(dim(ans<-x[, lapply(.SD, mean, na.rm=TRUE), by=id4, .SDcols=v1:v3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(v2), sum(v3))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, lapply(.SD, mean, na.rm=TRUE), by=id4, .SDcols=v1:v3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(v2), sum(v3))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1:v3 by id6" # q5
t = system.time(print(dim(ans<-x[, lapply(.SD, sum, na.rm=TRUE), by=id6, .SDcols=v1:v3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)), sum(bit64::as.integer64(v2)), sum(v3))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, lapply(.SD, sum, na.rm=TRUE), by=id6, .SDcols=v1:v3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)), sum(bit64::as.integer64(v2)), sum(v3))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "median v3 sd v3 by id4 id5" # q6
t = system.time(print(dim(ans<-x[, .(median_v3=median(v3, na.rm=TRUE), sd_v3=sd(v3, na.rm=TRUE)), by=.(id4, id5)])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(median_v3), sum(sd_v3))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, .(median_v3=median(v3, na.rm=TRUE), sd_v3=sd(v3, na.rm=TRUE)), by=.(id4, id5)])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(median_v3), sum(sd_v3))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "max v1 - min v2 by id3" # q7
t = system.time(print(dim(ans<-x[, .(range_v1_v2=max(v1, na.rm=TRUE)-min(v2, na.rm=TRUE)), by=id3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(range_v1_v2)))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, .(range_v1_v2=max(v1, na.rm=TRUE)-min(v2, na.rm=TRUE)), by=id3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(range_v1_v2)))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "largest two v3 by id6" # q8
t = system.time(print(dim(ans<-x[order(-v3, na.last=NA), .(largest2_v3=head(v3, 2L)), by=id6])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(largest2_v3))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[order(-v3, na.last=NA), .(largest2_v3=head(v3, 2L)), by=id6])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(largest2_v3))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "regression v1 v2 by id2 id4" # q9
t = system.time(print(dim(ans<-x[, .(r2=cor(v1, v2, use="complete.obs")^2), by=.(id2, id4)])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(r2))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, .(r2=cor(v1, v2, use = "complete.obs")^2), by=.(id2, id4)])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(r2))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v3 count by id1:id6" # q10
t = system.time(print(dim(ans<-x[, .(v3=sum(v3, na.rm=TRUE), count=.N), by=id1:id6])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v3), sum(bit64::as.integer64(count)))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[, .(v3=sum(v3, na.rm=TRUE), count=.N), by=id1:id6])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v3), sum(bit64::as.integer64(count)))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

cat(sprintf("grouping finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
