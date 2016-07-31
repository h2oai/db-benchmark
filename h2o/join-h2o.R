#!/usr/bin/env Rscript

cat("# join-h2o.R\n")

source("./helpers.R")
source("./h2o/helpers-h2o.R")

library(h2o, warn.conflicts=FALSE, quietly=TRUE)
h2o.init(ip=Sys.getenv("H2O_HOST","localhost"), port=as.integer(Sys.getenv("H2O_PORT","54321")), strict_version_check=FALSE, startH2O=FALSE)
h2o.removeAll()
ver = h2o.getVersion()
git = h2o.git()
task = "join"
solution = "h2o"
fun = "h2o.merge"
question = "inner join"
cache = TRUE

src_x = Sys.getenv("SRC_X")
src_y = Sys.getenv("SRC_Y")
data_name = paste(basename(src_x), basename(src_y), sep="-")
X = h2o.importFile(src_x)
Y = h2o.importFile(src_y)

t = system.time(print(dim(ans<-h2o.merge(X, Y, method="radix"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(X2=bit64::as.integer64(sum(ans[["X2"]])), Y2=bit64::as.integer64(sum(ans[["Y2"]]))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
h2o.rm(ans)

t = system.time(print(dim(ans<-h2o.merge(X, Y, method="radix"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(X2=bit64::as.integer64(sum(ans[["X2"]])), Y2=bit64::as.integer64(sum(ans[["Y2"]]))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
h2o.rm(ans)

t = system.time(print(dim(ans<-h2o.merge(X, Y, method="radix"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-list(X2=bit64::as.integer64(sum(ans[["X2"]])), Y2=bit64::as.integer64(sum(ans[["Y2"]]))))[["elapsed"]]
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
h2o.rm(ans)

h2o.removeAll()
if( !interactive() ) q("no", status=0)
