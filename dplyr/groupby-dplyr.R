#!/usr/bin/env Rscript

cat("# groupby-dplyr.R\n")

source("./helpers.R")
source("./dplyr/helpers-dplyr.R")

stopifnot(requireNamespace(c("bit64","data.table"), quietly=TRUE)) # used in chk to sum numeric columns and data loading
suppressPackageStartupMessages(library(dplyr, warn.conflicts=FALSE))
ver = packageVersion("dplyr")
git = dplyr.git()
task = "groupby"
solution = "dplyr"
fun = "group_by"
cache = TRUE

data_name = gsub(".fst", "", Sys.getenv("SRC_GRP_LOCAL"), fixed=TRUE)
src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

X = as_tibble(data.table::fread(src_grp, showProgress=FALSE, stringsAsFactors=TRUE, data.table=FALSE))
print(nrow(X))

cat("grouping...\n")

question = "sum v1 by id1" # q1
t = system.time(print(dim(ans<-summarise(group_by(X, id1), v1=sum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-summarise(group_by(X, id1), v1=sum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

question = "sum v1 by id1:id2" # q2
t = system.time(print(dim(ans<-summarise(group_by(X, id1, id2), v1=sum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-summarise(group_by(X, id1, id2), v1=sum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

question = "sum v1 mean v3 by id3" # q3
t = system.time(print(dim(ans<-summarise(group_by(X, id3), v1=sum(v1), v3=mean(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1)), v3=sum(v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-summarise(group_by(X, id3), v1=sum(v1), v3=mean(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1)), v3=sum(v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

question = "mean v1:v3 by id4" # q4
t = system.time(print(dim(ans<-summarise_at(group_by(X, id4), .fun="mean", .vars=c("v1","v2","v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(v1), v2=sum(v2), v3=sum(v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-summarise_at(group_by(X, id4), .fun="mean", .vars=c("v1","v2","v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(v1), v2=sum(v2), v3=sum(v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

question = "sum v1:v3 by id6" # q5
t = system.time(print(dim(ans<-summarise_at(group_by(X, id6), .funs="sum", .vars=c("v1","v2","v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1)), v2=sum(bit64::as.integer64(v2)), v3=sum(v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-summarise_at(group_by(X, id6), .funs="sum", .vars=c("v1","v2","v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1)), v2=sum(bit64::as.integer64(v2)), v3=sum(v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

question = "sum v3 count by id1:id6" # q6
t = system.time(print(dim(ans<-summarise(group_by(X, id1, id2, id3, id4, id5, id6), v3=sum(v3), count=n()))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v3=sum(v3), count=sum(bit64::as.integer64(count))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-summarise(group_by(X, id1, id2, id3, id4, id5, id6), v3=sum(v3), count=n()))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v3=sum(v3), count=sum(bit64::as.integer64(count))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

if( !interactive() ) q("no", status=0)
