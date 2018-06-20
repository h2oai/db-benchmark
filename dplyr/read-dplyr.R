#!/usr/bin/env Rscript

cat("# read-dplyr\n")

source("./helpers.R")
source("./dplyr/helpers-dplyr.R")

suppressPackageStartupMessages({
  library(readr, warn.conflicts=FALSE)
  library(dplyr, warn.conflicts=FALSE)
})
ver = NA_character_ #packageVersion("dplyr")
git = NA_character_ #dplyr.git()
task = "read"
solution = "dplyr"
fun = "readr::read_csv"
cache = TRUE

src_grp = Sys.getenv("SRC_GRP")
data_name = basename(src_grp)
options("readr.show_progress"=FALSE)

cat("reading...\n")

question = "all rows" #1
t = system.time(print(dim(ans<-read_csv(data_name, col_types="ccciiiiid"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v3=sum(v1)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-read_csv(data_name, col_types="ccciiiiid"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v3=sum(v1)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-read_csv(data_name, col_types="ccciiiiid"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v3=sum(v1)))[["elapsed"]]
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

question = "top 100 rows" #2
t = system.time(print(dim(ans<-read_csv(data_name, n_max=100, col_types="ccciiiiid"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v3=sum(v1)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-read_csv(data_name, n_max=100, col_types="ccciiiiid"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v3=sum(v1)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-read_csv(data_name, n_max=100, col_types="ccciiiiid"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ungroup(ans), v3=sum(v1)))[["elapsed"]]
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

if( !interactive() ) q("no", status=0)
