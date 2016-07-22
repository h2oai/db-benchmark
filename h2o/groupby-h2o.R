#!/usr/bin/env Rscript

cat("# groupby-h2o.R\n")

source("./helpers.R")
source("./h2o/helpers-h2o.R")

library(h2o, warn.conflicts=FALSE, quietly=TRUE)
h2o.init(ip=Sys.getenv("H2O_HOST","localhost"), port=as.integer(Sys.getenv("H2O_PORT","54321")), strict_version_check=FALSE, startH2O=FALSE)
h2o.removeAll()
ver = h2o.getVersion()
git = h2o.git()
task = "groupby"
solution = "h2o"
fun = "h2o.group_by"

data_name = "G1_1e7_1e2.csv"
X = h2o.uploadFile(normalizePath(data_name))

question = "sum v1 by id1" #1
t = system.time(ans<-h2o.group_by(X, by="id1", sum("v1")))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)
t = system.time(ans<-h2o.group_by(X, by="id1", sum("v1")))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)
t = system.time(ans<-h2o.group_by(X, by="id1", sum("v1")))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)

question = "sum v1 by id1:id2" #2
t = system.time(ans<-h2o.group_by(X, by=c("id1","id2"), sum("v1")))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)
t = system.time(ans<-h2o.group_by(X, by=c("id1","id2"), sum("v1")))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)
t = system.time(ans<-h2o.group_by(X, by=c("id1","id2"), sum("v1")))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)

question = "sum v1 mean v3 by id3" #3
t = system.time(ans<-h2o.group_by(X, by="id3", sum("v1"), mean("v3")))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)
t = system.time(ans<-h2o.group_by(X, by="id3", sum("v1"), mean("v3")))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)
t = system.time(ans<-h2o.group_by(X, by="id3", sum("v1"), mean("v3")))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)

question = "mean v1:v3 by id4" #4
t = system.time(ans<-h2o.group_by(X, by="id4", mean("v1"), mean("v2"), mean("v3")))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)
t = system.time(ans<-h2o.group_by(X, by="id4", mean("v1"), mean("v2"), mean("v3")))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)
t = system.time(ans<-h2o.group_by(X, by="id4", mean("v1"), mean("v2"), mean("v3")))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)

question = "sum v1:v3 by id6" #5
t = system.time(ans<-h2o.group_by(X, by="id6", sum("v1"), sum("v2"), sum("v3")))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)
t = system.time(ans<-h2o.group_by(X, by="id6", sum("v1"), sum("v2"), sum("v3")))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)
t = system.time(ans<-h2o.group_by(X, by="id6", sum("v1"), sum("v2"), sum("v3")))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m)
h2o.rm(ans)

h2o.removeAll()
if( !interactive() ) q("no", status=0)
