#!/usr/bin/env Rscript

cat("# join-h2o.R\n")

source("./helpers.R")
source("./h2o/helpers-h2o.R")

library(h2o, warn.conflicts=FALSE, quietly=TRUE)
h2o.init(ip=Sys.getenv("H2O_HOST","localhost"), port=as.integer(Sys.getenv("H2O_PORT","54321")), strict_version_check=FALSE, startH2O=FALSE)
h2o.removeAll()
ver = h2o.getVersion()
git = h2o.git()

src_x = Sys.getenv("SRC_X")
src_y = Sys.getenv("SRC_Y")
X = h2o.importFile(src_x)
Y = h2o.importFile(src_y)

t = system.time(dim(hf<-h2o.merge(X, Y, method="radix")))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task="join", data="", in_rows=nrow(X), out_rows=nrow(hf), solution="h2o", version=ver, git=git, fun="h2o.merge", time_sec=t, mem_gb=m)
h2o.rm(hf)

t = system.time(dim(hf<-h2o.merge(X, Y, method="radix")))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task="join", data="", in_rows=nrow(X), out_rows=nrow(hf), solution="h2o", version=ver, git=git, fun="h2o.merge", time_sec=t, mem_gb=m)
h2o.rm(hf)

t = system.time(dim(hf<-h2o.merge(X, Y, method="radix")))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task="join", data="", in_rows=nrow(X), out_rows=nrow(hf), solution="h2o", version=ver, git=git, fun="h2o.merge", time_sec=t, mem_gb=m)
h2o.rm(hf)

if( !interactive() ) q("no", status=0)
