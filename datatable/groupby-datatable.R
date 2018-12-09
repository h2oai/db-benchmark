#!/usr/bin/env Rscript

cat("# groupby-datatable.R\n")

source("./helpers.R")
source("./datatable/helpers-datatable.R")

stopifnot(requireNamespace(c("bit64","fst"), quietly=TRUE)) # used in chk to sum numeric columns
suppressPackageStartupMessages(library(data.table))
ver = packageVersion("data.table")
git = datatable.git()
task = "groupby"
solution = "data.table"
fun = "[.data.table"
cache = TRUE

src_grp = Sys.getenv("SRC_GRP_LOCAL")
data_name = substr(src_grp, 1, nchar(src_grp)-4)
cat(sprintf("loading dataset %s\n", data_name))

X = setDT(fst::read.fst(file.path("data", src_grp)))
print(nrow(X))

cat("grouping...\n")

question = "sum v1 by id1" # q1
t = system.time(print(dim(ans<-X[, .(v1=sum(v1)), by=id1])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-X[, .(v1=sum(v1)), by=id1])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

question = "sum v1 by id1:id2" # q2
t = system.time(print(dim(ans<-X[, .(v1=sum(v1)), by=.(id1, id2)])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-X[, .(v1=sum(v1)), by=.(id1, id2)])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

question = "sum v1 mean v3 by id3" # q3
t = system.time(print(dim(ans<-X[, .(v1=sum(v1), v3=mean(v3)), by=id3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)), sum(v3))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-X[, .(v1=sum(v1), v3=mean(v3)), by=id3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)), sum(v3))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

question = "mean v1:v3 by id4" # q4
t = system.time(print(dim(ans<-X[, lapply(.SD, mean), by=id4, .SDcols=v1:v3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(v2), sum(v3))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-X[, lapply(.SD, mean), by=id4, .SDcols=v1:v3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(v2), sum(v3))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

question = "sum v1:v3 by id6" # q5
t = system.time(print(dim(ans<-X[, lapply(.SD, sum), by=id6, .SDcols=v1:v3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)), sum(bit64::as.integer64(v2)), sum(v3))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-X[, lapply(.SD, sum), by=id6, .SDcols=v1:v3])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(v1)), sum(bit64::as.integer64(v2)), sum(v3))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

question = "sum v3 count by id1:id6" # q6
t = system.time(print(dim(ans<-X[, .(v3=sum(v3), count=.N), by=id1:id6])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v3), sum(bit64::as.integer64(count)))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-X[, .(v3=sum(v3), count=.N), by=id1:id6])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v3), sum(bit64::as.integer64(count)))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

if( !interactive() ) q("no", status=0)
