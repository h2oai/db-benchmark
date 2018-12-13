#!/usr/bin/env Rscript

cat("# groupby-datatable.R\n")

source("./helpers.R")
source("./datatable/helpers-datatable.R")

stopifnot(requireNamespace(c("bit64"), quietly=TRUE)) # used in chk to sum numeric columns
suppressPackageStartupMessages(library(data.table))
ver = packageVersion("data.table")
git = datatable.git()
task = "groupby"
solution = "data.table"
fun = "[.data.table"
cache = TRUE

data_name = gsub(".fst", "", Sys.getenv("SRC_GRP_LOCAL"), fixed=TRUE)
src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

X = fread(src_grp, showProgress=FALSE, stringsAsFactors=TRUE)
print(nrow(X))

cat("grouping...\n")

question = "sum v1 by id1" #1
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

question = "sum v1 by id1:id2" #2
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

question = "sum v1 mean v3 by id3" #3
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

question = "mean v1:v3 by id4" #4
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

question = "sum v1:v3 by id6" #5
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

if( !interactive() ) q("no", status=0)
