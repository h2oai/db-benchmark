#!/usr/bin/env Rscript

cat("# join-datatable.R\n")

source("./_helpers/helpers.R")

suppressPackageStartupMessages(library("data.table", lib.loc="./datatable/r-datatable"))
setDTthreads(0L)
ver = packageVersion("data.table")
git = data.table:::.git(quiet=TRUE)
task = "join"
solution = "data.table"
fun = "[.data.table"
cache = TRUE
on_disk = FALSE

data_name = Sys.getenv("SRC_DATANAME")
src_jn_x = file.path("data", paste(data_name, "csv", sep="."))
y_data_name = join_to_tbls(data_name)
src_jn_y = setNames(file.path("data", paste(y_data_name, "csv", sep=".")), names(y_data_name))
stopifnot(length(src_jn_y)==3L)
cat(sprintf("loading datasets %s\n", paste(c(data_name, y_data_name), collapse=", ")))

x = fread(src_jn_x, showProgress=FALSE, stringsAsFactors=TRUE, na.strings="")
JN = sapply(simplify=FALSE, src_jn_y, fread, showProgress=FALSE, stringsAsFactors=TRUE, na.strings="")
print(nrow(x))
sapply(sapply(JN, nrow), print) -> nul
small = JN$small
medium = JN$medium
big = JN$big

task_init = proc.time()[["elapsed"]]
cat("joining...\n")

question = "small inner on int" # q1
t = system.time(print(dim(ans<-x[small, on="id1", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[small, on="id1", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium inner on int" # q2
t = system.time(print(dim(ans<-x[medium, on="id2", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[medium, on="id2", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium outer on int" # q3
t = system.time(print(dim(ans<-medium[x, on="id2"])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-medium[x, on="id2"])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium inner on factor" # q4
t = system.time(print(dim(ans<-x[medium, on="id5", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[medium, on="id5", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "big inner on int" # q5
t = system.time(print(dim(ans<-x[big, on="id3", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x[big, on="id3", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

## advanced questions

#question = "medium inner on int int" # q6
#t = system.time(print(dim(ans<-x[medium, on=.(id1,id2), nomatch=NULL])))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
##write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#rm(ans)
#t = system.time(print(dim(ans<-x[medium, on=.(id1,id2), nomatch=NULL])))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
##write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(head(ans, 3))
#print(tail(ans, 3))
#rm(ans)

#question = "medium update on int" # q7
#t = system.time(print(dim(ans<-x[medium, v2:=i.v2, on="id2"])))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
##write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#rm(ans)
#x[, v2:=NULL]
#t = system.time(print(dim(ans<-x[medium, v2:=i.v2, on="id2"])))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
##write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(head(ans, 3))
#print(tail(ans, 3))
#rm(ans)
#x[, v2:=NULL]

## DEV advanced questions

#question = "medium aggregate on int" # q8
#stop("dev q8: ", question)
#t = system.time(print(dim(ans<-x[medium, on="id2", .(count=.N, v2=sum(v2)), by=.EACHI, nomatch=NULL])))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
#write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#rm(ans)
#t = system.time(print(dim(ans<-x[medium, on=.(id1,id2), nomatch=NULL])))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-ans[, .(sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))])[["elapsed"]]
#write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(head(ans, 3))
#print(tail(ans, 3))
#rm(ans)

#question = "medium rolling on int" # q9
#stop("dev q9: ", question)
#t = system.time(print(dim(ans<-x[medium, on="id2", .(count=.N, v2=sum(v2)),])))[["elapsed"]]

#question = "big non-equi aggregate on int int int" # q10
#stop("dev q10: ", question)
#t = system.time(print(dim(ans<-x[big, on=.(id1, id2, id3>=id3), .N, by=.EACHI, nomatch=NULL])))[["elapsed"]]

cat(sprintf("joining finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if (!interactive()) q("no", status=0)
