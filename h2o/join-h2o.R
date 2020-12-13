#!/usr/bin/env Rscript

cat("# join-h2o.R\n")

source("./_helpers/helpers.R")

suppressPackageStartupMessages(library("h2o", lib.loc="./h2o/r-h2o", warn.conflicts=FALSE, quietly=TRUE))
ver = packageVersion("h2o")
git = ""
task = "join"
solution = "h2o"
fun = "h2o.merge"
cache = TRUE
on_disk = FALSE

h = h2o.init(startH2O=FALSE, port=55888)
h2o.no_progress()

data_name = Sys.getenv("SRC_DATANAME")
src_jn_x = file.path("data", paste(data_name, "csv", sep="."))
y_data_name = join_to_tbls(data_name)
src_jn_y = setNames(file.path("data", paste(y_data_name, "csv", sep=".")), names(y_data_name))
stopifnot(length(src_jn_y)==3L)
cat(sprintf("loading datasets %s\n", paste(c(data_name, y_data_name), collapse=", ")))

x = h2o.importFile(src_jn_x, col.types=c("int","int","int","enum","enum","string","real"))
print(nrow(x))
small = h2o.importFile(src_jn_y[1L], col.types=c("int","enum","real"))
medium = h2o.importFile(src_jn_y[2L], col.types=c("int","int","enum","enum","real"))
big = h2o.importFile(src_jn_y[3L], col.types=c("int","int","int","enum","enum","string","real"))
sapply(sapply(list(small, medium, big), nrow), print) -> nul

task_init = proc.time()[["elapsed"]]
cat("joining...\n")

question = "small inner on int" # q1

t = system.time(print(dim(ans<-h2o.merge(x, small, by="id1"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["v1"]]), sum(ans[["v2"]])))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
h2o.rm(ans)
t = system.time(print(dim(ans<-h2o.merge(x, small, by="id1"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["v1"]]), sum(ans[["v2"]])))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
h2o.rm(ans)

question = "medium inner on int" # q2
t = system.time(print(dim(ans<-h2o.merge(x, medium, by="id2"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["v1"]]), sum(ans[["v2"]])))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
h2o.rm(ans)
t = system.time(print(dim(ans<-h2o.merge(x, medium, by="id2"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["v1"]]), sum(ans[["v2"]])))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
h2o.rm(ans)

question = "medium outer on int" # q3
t = system.time(print(dim(ans<-h2o.merge(x, medium, by="id2", all.x=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["v1"]]), sum(ans[["v2"]], na.rm=TRUE)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
h2o.rm(ans)
t = system.time(print(dim(ans<-h2o.merge(x, medium, by="id2", all.x=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["v1"]]), sum(ans[["v2"]], na.rm=TRUE)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
h2o.rm(ans)

question = "medium inner on factor" # q4
t = system.time(print(dim(ans<-h2o.merge(x, medium, by="id5"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["v1"]]), sum(ans[["v2"]])))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
h2o.rm(ans)
t = system.time(print(dim(ans<-h2o.merge(x, medium, by="id5"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["v1"]]), sum(ans[["v2"]])))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
h2o.rm(ans)

question = "big inner on int" # q5
t = system.time(print(dim(ans<-h2o.merge(x, big, by="id3"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["v1"]]), sum(ans[["v2"]])))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
h2o.rm(ans)
t = system.time(print(dim(ans<-h2o.merge(x, big, by="id3"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["v1"]]), sum(ans[["v2"]])))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
h2o.rm(ans)

h2o.removeAll()

cat(sprintf("joining finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if (!interactive()) q("no", status=0)
