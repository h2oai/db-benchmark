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

data_name = Sys.getenv("SRC_GRP_LOCAL")
src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

DF = as_tibble(data.table::fread(src_grp, showProgress=FALSE, stringsAsFactors=TRUE, data.table=FALSE))
print(nrow(DF))

cat("grouping...\n")

question = "sum v1 by id1" # q1
t = system.time(print(dim(ans<-DF %>% group_by(id1, .drop=TRUE) %>% summarise(v1=sum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DF %>% group_by(id1, .drop=TRUE) %>% summarise(v1=sum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 by id1:id2" # q2
t = system.time(print(dim(ans<-DF %>% group_by(id1, id2, .drop=TRUE) %>% summarise(v1=sum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DF %>% group_by(id1, id2, .drop=TRUE) %>% summarise(v1=sum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 mean v3 by id3" # q3
t = system.time(print(dim(ans<-DF %>% group_by(id3, .drop=TRUE) %>% summarise(v1=sum(v1), v3=mean(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1)), v3=sum(v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DF %>% group_by(id3, .drop=TRUE) %>% summarise(v1=sum(v1), v3=mean(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1)), v3=sum(v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "mean v1:v3 by id4" # q4
t = system.time(print(dim(ans<-DF %>% group_by(id4, .drop=TRUE) %>% summarise_at(.fun="mean", .vars=c("v1","v2","v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(v1), v2=sum(v2), v3=sum(v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DF %>% group_by(id4, .drop=TRUE) %>% summarise_at(.fun="mean", .vars=c("v1","v2","v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(v1), v2=sum(v2), v3=sum(v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1:v3 by id6" # q5
t = system.time(print(dim(ans<-DF %>% group_by(id6, .drop=TRUE) %>% summarise_at(.funs="sum", .vars=c("v1","v2","v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1)), v2=sum(bit64::as.integer64(v2)), v3=sum(v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DF %>% group_by(id6, .drop=TRUE) %>% summarise_at(.funs="sum", .vars=c("v1","v2","v3")))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v1=sum(bit64::as.integer64(v1)), v2=sum(bit64::as.integer64(v2)), v3=sum(v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "median v3 sd v3 by id2 id4" # q6
t = system.time(print(dim(ans<-DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(median_v3=median(v3), sd_v3=sd(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), median_v3=sum(median_v3), sd_v3=sum(sd_v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(median_v3=median(v3), sd_v3=sd(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), median_v3=sum(median_v3), sd_v3=sum(sd_v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "max v1 - min v2 by id2 id4" # q7
t = system.time(print(dim(ans<-DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(range_v1_v2=max(v1)-min(v2)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), range_v1_v2=sum(bit64::as.integer64(range_v1_v2))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(range_v1_v2=max(v1)-min(v2)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), range_v1_v2=sum(bit64::as.integer64(range_v1_v2))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "largest two v3 by id2 id4" # q8
t = system.time(print(dim(ans<-DF %>% select(id2, id4, largest2_v3=v3) %>% arrange(desc(largest2_v3)) %>% group_by(id2, id4, .drop=TRUE) %>% filter(row_number() <= 2L))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), largest2_v3=sum(largest2_v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DF %>% select(id2, id4, largest2_v3=v3) %>% arrange(desc(largest2_v3)) %>% group_by(id2, id4, .drop=TRUE) %>% filter(row_number() <= 2L))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), largest2_v3=sum(largest2_v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "regression v1 v2 by id2 id4" # q9
t = system.time(print(dim(ans<-DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(r2=cor(v1, v2)^2))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), r2=sum(r2)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(r2=cor(v1, v2)^2))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), r2=sum(r2)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v3 count by id1:id6" # q10
t = system.time(print(dim(ans<-DF %>% group_by(id1, id2, id3, id4, id5, id6, .drop=TRUE) %>% summarise(v3=sum(v3), count=n()))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v3=sum(v3), count=sum(bit64::as.integer64(count))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-DF %>% group_by(id1, id2, id3, id4, id5, id6, .drop=TRUE) %>% summarise(v3=sum(v3), count=n()))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v3=sum(v3), count=sum(bit64::as.integer64(count))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(DF), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

if( !interactive() ) q("no", status=0)
