#!/usr/bin/env Rscript

cat("# join-dplyr.R\n")

source("./helpers.R")
source("./dplyr/helpers-dplyr.R")

suppressPackageStartupMessages(library(dplyr, warn.conflicts=FALSE))
ver = packageVersion("dplyr")
git = dplyr.git()
task = "join"
solution = "dplyr"
cache = TRUE

data_name = Sys.getenv("SRC_JN_LOCAL")
src_jn_x = file.path("data", paste(data_name, "csv", sep="."))
y_data_name = join_to_tbls(data_name)
src_jn_y = setNames(file.path("data", paste(y_data_name, "csv", sep=".")), names(y_data_name))
stopifnot(length(src_jn_y)==3L)
cat(sprintf("loading datasets %s\n", paste(c(data_name, y_data_name), collapse=", ")))

DF = as_tibble(data.table::fread(src_jn_x, showProgress=FALSE, stringsAsFactors=TRUE, data.table=FALSE))
JN = lapply(sapply(simplify=FALSE, src_jn_y, data.table::fread, showProgress=FALSE, stringsAsFactors=TRUE, data.table=FALSE), as_tibble)
print(nrow(DF))
sapply(sapply(JN, nrow), print) -> nul
small = JN$small
medium = JN$medium
big = JN$big

task_init = proc.time()[["elapsed"]]
cat("joining...\n")

question = "small inner on int" # q1
fun = "inner_join"
t = system.time(print(dim(ans<-inner_join(DF, small, by="id1"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(v1), sum(v2)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-inner_join(DF, small, by="id1"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(v1), sum(v2)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium inner on int" # q2
fun = "inner_join"
t = system.time(print(dim(ans<-inner_join(DF, medium, by="id2"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(v1), sum(v2)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-inner_join(DF, medium, by="id2"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(v1), sum(v2)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium outer on int" # q3
fun = "left_join"
t = system.time(print(dim(ans<-left_join(DF, medium, by="id2"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(v1), sum(v2, na.rm=TRUE)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-left_join(DF, medium, by="id2"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(v1), sum(v2, na.rm=TRUE)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium inner on factor" # q4
fun = "inner_join"
t = system.time(print(dim(ans<-inner_join(DF, medium, by="id5"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(v1), sum(v2)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-inner_join(DF, medium, by="id5"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(v1), sum(v2)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "big inner on int" # q5
fun = "inner_join"
t = system.time(print(dim(ans<-inner_join(DF, big, by="id3"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(v1), sum(v2)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-inner_join(DF, big, by="id3"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(v1), sum(v2)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

cat(sprintf("joining finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
