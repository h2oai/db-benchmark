#!/usr/bin/env Rscript

cat("# groupby-h2o.R\n")

source("./_helpers/helpers.R")

suppressPackageStartupMessages(library("h2o", lib.loc="./h2o/r-h2o", warn.conflicts=FALSE, quietly=TRUE))
ver = packageVersion("h2o")
git = ""
task = "groupby"
solution = "h2o"
fun = "h2o.group_by"
cache = TRUE
on_disk = FALSE

h = h2o.init(startH2O=FALSE, port=55888)
h2o.no_progress()

data_name = Sys.getenv("SRC_GRP_LOCAL")
src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

x = h2o.importFile(src_grp)
# Could not use factor columns due to ERROR caused by water.parser.ParseDataset$H2OParseException: Exceeded categorical limit on column #3 (using 1-based indexing).  Consider reparsing this column as a string. https://0xdata.atlassian.net/browse/PUBDEV-7533
# Could not use string columns due to ERROR caused by java.lang.IllegalArgumentException: Operation not allowed on string vector. https://0xdata.atlassian.net/browse/PUBDEV-7536
print(nrow(x))

task_init = proc.time()[["elapsed"]]
cat("grouping...\n")

question = "sum v1 by id1" # q1
t = system.time(print(dim(ans<-h2o.group_by(x, by="id1", sum("v1")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(v1=sum(ans[["sum_v1"]])))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
h2o.rm(ans)
t = system.time(print(dim(ans<-h2o.group_by(x, by="id1", sum("v1")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(v1=sum(ans[["sum_v1"]])))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
h2o.rm(ans)

question = "sum v1 by id1:id2" # q2
t = system.time(print(dim(ans<-h2o.group_by(x, by=c("id1","id2"), sum("v1")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(v1=sum(ans[["sum_v1"]])))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
h2o.rm(ans)
t = system.time(print(dim(ans<-h2o.group_by(x, by=c("id1","id2"), sum("v1")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(v1=sum(ans[["sum_v1"]])))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
h2o.rm(ans)

question = "sum v1 mean v3 by id3" # q3
t = system.time(print(dim(ans<-h2o.group_by(x, by="id3", sum("v1"), mean("v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(v1=sum(ans[["sum_v1"]]), v3=sum(ans[["mean_v3"]])))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
h2o.rm(ans)
t = system.time(print(dim(ans<-h2o.group_by(x, by="id3", sum("v1"), mean("v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(v1=sum(ans[["sum_v1"]]), v3=sum(ans[["mean_v3"]])))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
h2o.rm(ans)

question = "mean v1:v3 by id4" # q4
t = system.time(print(dim(ans<-h2o.group_by(x, by="id4", mean("v1"), mean("v2"), mean("v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(v1=sum(ans[["mean_v1"]]), v2=sum(ans[["mean_v2"]]), v3=sum(ans[["mean_v3"]])))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
h2o.rm(ans)
t = system.time(print(dim(ans<-h2o.group_by(x, by="id4", mean("v1"), mean("v2"), mean("v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(v1=sum(ans[["mean_v1"]]), v2=sum(ans[["mean_v2"]]), v3=sum(ans[["mean_v3"]])))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
h2o.rm(ans)

question = "sum v1:v3 by id6" # q5
t = system.time(print(dim(ans<-h2o.group_by(x, by="id6", sum("v1"), sum("v2"), sum("v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(v1=sum(ans[["sum_v1"]]), v2=sum(ans[["sum_v2"]]), v3=sum(ans[["sum_v3"]])))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
h2o.rm(ans)
t = system.time(print(dim(ans<-h2o.group_by(x, by="id6", sum("v1"), sum("v2"), sum("v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(v1=sum(ans[["sum_v1"]]), v2=sum(ans[["sum_v2"]]), v3=sum(ans[["sum_v3"]])))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
h2o.rm(ans)

question = "median v3 sd v3 by id4 id5" # q6
t = system.time(print(dim(ans<-h2o.group_by(x, by=c("id4","id5"), median("v3"), sd("v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["median_v3"]]), sum(ans[["sd_v3"]])))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
h2o.rm(ans)
t = system.time(print(dim(ans<-h2o.group_by(x, by=c("id4","id5"), median("v3"), sd("v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["median_v3"]]), sum(ans[["sd_v3"]])))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
h2o.rm(ans)

question = "max v1 - min v2 by id3" # q7
#https://0xdata.atlassian.net/browse/PUBDEV-7538

question = "largest two v3 by id6" # q8
#https://0xdata.atlassian.net/browse/PUBDEV-7539

question = "regression v1 v2 by id2 id4" # q9
#https://0xdata.atlassian.net/browse/PUBDEV-7540

question = "sum v3 count by id1:id6" # q10
t = system.time(print(dim(ans<-h2o.group_by(x, by=c("id1","id2","id3","id4","id5","id6"), sum("v3"), nrow("id1","id2","id3","id4","id5","id6")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["sum_v3"]]), sum(ans[["nrow"]])))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
h2o.rm(ans)
t = system.time(print(dim(ans<-h2o.group_by(x, by=c("id1","id2","id3","id4","id5","id6"), sum("v3"), nrow("id1","id2","id3","id4","id5","id6")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(sum(ans[["sum_v3"]]), sum(ans[["nrow"]])))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
h2o.rm(ans)

h2o.removeAll()

cat(sprintf("grouping finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if (!interactive()) q("no", status=0)
