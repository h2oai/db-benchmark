#!/usr/bin/env Rscript

cat("# join-arrow\n")

source("./_helpers/helpers.R")

.libPaths("./arrow/r-arrow") # tidyverse/dplyr#4641 ## leave it like here in case if this affects arrow pkg as well
suppressPackageStartupMessages({
  library("arrow", lib.loc="./arrow/r-arrow", warn.conflicts=FALSE)
  library("dplyr", lib.loc="./arrow/r-arrow", warn.conflicts=FALSE)
})
ver = packageVersion("arrow")
git = ""
task = "join"
solution = "arrow"
cache = TRUE
on_disk = FALSE

data_name = Sys.getenv("SRC_DATANAME")
src_jn_x = file.path("data", paste(data_name, "csv", sep="."))
y_data_name = join_to_tbls(data_name)
src_jn_y = setNames(file.path("data", paste(y_data_name, "csv", sep=".")), names(y_data_name))
stopifnot(length(src_jn_y)==3L)
cat(sprintf("loading datasets %s\n", paste(c(data_name, y_data_name), collapse=", ")))

# message("join method not yet implemented for arrow #189")
# q("no", 1L)

x = read_csv_arrow(src_jn_x, schema = schema(id1=int32(),id2=int32(),id3=int32(),id4=string(),id5=string(),id6=string(),v1=double()), skip=1, as_data_frame=FALSE)
small = read_csv_arrow(src_jn_y[1L], schema = schema(id1=int32(),id4=string(),v2=double()), skip=1, as_data_frame=FALSE)
medium = read_csv_arrow(src_jn_y[2L], schema = schema(id1=int32(),id2=int32(),id4=string(),id5=string(),v2=double()), skip=1, as_data_frame=FALSE)
big = read_csv_arrow(src_jn_y[3L], schema = schema(id1=int32(),id2=int32(),id3=int32(),id4=string(),id5=string(),id6=string(),v2=double()), skip=1, as_data_frame=FALSE)
print(nrow(x))
print(nrow(small))
print(nrow(medium))
print(nrow(big))

task_init = proc.time()[["elapsed"]]
cat("joining...\n")

question = "small inner on int" # q1
fun = "inner_join"
t = system.time({
  ans<-collect(inner_join(x, small, by="id1"))
  print(dim(ans))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk <- collect(summarise(ans, sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time({
  ans<-collect(inner_join(x, small, by="id1"))
  print(dim(ans))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk <- collect(summarise(ans, sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans <- collect(ans)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium inner on int" # q2
fun = "inner_join"
t = system.time({
  ans<-collect(inner_join(x, medium, by="id2"))
  print(dim(ans))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk <- collect(summarise(ans, sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time({
  ans<-collect(inner_join(x, medium, by="id2"))
  print(dim(ans))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk <- collect(summarise(ans, sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans <- collect(ans)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium outer on int" # q3
fun = "left_join"
t = system.time({
  ans<-collect(left_join(x, medium, by="id2"))
  print(dim(ans))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk <- collect(summarise(ans, sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time({
  ans<-collect(left_join(x, medium, by="id2"))
  print(dim(ans))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk <- collect(summarise(ans, sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans <- collect(ans)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "medium inner on factor" # q4
fun = "inner_join"
t = system.time({
  ans <- collect(inner_join(x, medium, by="id5"))
  print(dim(ans))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk <- collect(summarise(ans, sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time({
  ans <- collect(inner_join(x, medium, by="id5"))
  print(dim(ans))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk <- collect(summarise(ans, sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans <- collect(ans)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "big inner on int" # q5
fun = "inner_join"
t = system.time({
  ans<-collect(inner_join(x, big, by="id3"))
  print(dim(ans))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk <- collect(summarise(ans, sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time({
  ans<-collect(inner_join(x, big, by="id3"))
  print(dim(ans))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk <- collect(summarise(ans, sum(v1, na.rm=TRUE), sum(v2, na.rm=TRUE))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans <- collect(ans)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

cat(sprintf("joining finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)

