#!/usr/bin/env Rscript

cat("# join-datatable.R\n")

source("./helpers.R")
source("./datatable/helpers-datatable.R")

stopifnot(requireNamespace(c("bit64"), quietly=TRUE)) # used in chk to sum numeric columns
suppressPackageStartupMessages(library(data.table))
ver = packageVersion("data.table")
git = datatable.git()
task = "join"
solution = "data.table"
fun = "[.data.table"
cache = TRUE

data_name = "J1_1e6_NA_0_0" # Sys.getenv("SRC_JN_LOCAL")
src_jn_x = file.path("data", paste(data_name, "csv", sep="."))
y_data_name = join_to_tbls(data_name)
src_jn_y = setNames(file.path("data", paste(y_data_name, "csv", sep=".")), names(y_data_name))
stopifnot(length(src_jn_y)==3L)
cat(sprintf("loading datasets %s\n", paste(c(data_name, y_data_name), collapse=", ")))

DT = fread(src_jn_x, showProgress=FALSE, stringsAsFactors=TRUE)
JN = sapply(simplify=FALSE, src_jn_y, fread, showProgress=FALSE, stringsAsFactors=TRUE)
print(nrow(DT))
sapply(sapply(JN, nrow), print) -> nul

cat("joining...\n")

question = "big inner join on unique int" # q1
t = system.time(print(dim(ans<-DT[JN$big, on="id4", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(i.v1))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DT[JN$big, on="id4", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(i.v1))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium inner join on unique int" # q2
t = system.time(print(dim(ans<-DT[JN$medium, on="id4", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(i.v1))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DT[JN$medium, on="id4", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(i.v1))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "small inner join on unique int" # q3
t = system.time(print(dim(ans<-DT[JN$small, on="id4", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(i.v1))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DT[JN$small, on="id4", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(i.v1))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium outer join on unique int" # q4
t = system.time(print(dim(ans<-JN$medium[DT, on="id4", nomatch=NA])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(i.v1))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-JN$medium[DT, on="id4", nomatch=NA])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(i.v1))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium inner join on unique factor" # q5
t = system.time(print(dim(ans<-DT[JN$medium, on="id1", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(i.v1))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DT[JN$medium, on="id1", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(i.v1))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium inner join on unique int int" # q6
t = system.time(print(dim(ans<-DT[JN$medium, on=c("id4","id5"), nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(i.v1))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DT[JN$medium, on=c("id4","id5"), nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(i.v1))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium update on join on unique int" # q7
t = system.time(print(dim(ans <- DT[JN$medium, v2:=i.v1, on="id4"])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(v2))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
DT[, v2:=NULL]
t = system.time(print(dim(ans<-DT[JN$medium, v2:=i.v1, on="id4"])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1), sum(v2))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DT), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)
DT[, v2:=NULL]

if (!interactive()) q("no", status=0)
