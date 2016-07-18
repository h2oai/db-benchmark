#!/usr/bin/env Rscript

cat("# join-h2o.R\n")

memory_usage = function() {
  # not reliable
  res <- h2o:::.h2o.fromJSON(jsonlite::fromJSON(h2o:::.h2o.doSafeGET(urlSuffix = h2o:::.h2o.__CLOUD), simplifyDataFrame = FALSE))
  sum(sapply(res$nodes, function(x) x$max_mem - x$free_mem) / (1024^3))
}
write.log = function(
  timestamp=Sys.time(),
  task=NA_character_, data=NA_character_, in_rows=NA_integer_, out_rows=NA_integer_,
  solution=NA_character_, fun=NA_character_, run=NA_integer_, time_sec=NA_real_, mem_gb=NA_real_,
  log.file=Sys.getenv("CSV_TIME_FILE", "time.csv")
) {
  stopifnot(is.character(task), is.character(data), is.character(solution), is.character(fun))
  df=data.frame(timestamp=as.numeric(timestamp), 
                task=task, data=data, in_rows=as.integer(in_rows), out_rows=as.integer(out_rows),
                solution=solution, fun=fun, run=as.integer(run), time_sec=time_sec, mem_gb=mem_gb)
  cat("# ", paste(sapply(df, toString), collapse=","), "\n", sep="")
  write.table(df,
              file=log.file,
              append=file.exists(log.file),
              col.names=!file.exists(log.file),
              row.names=FALSE,
              quote=FALSE,
              sep=",")
}

library(h2o, warn.conflicts=FALSE, quietly=TRUE)
h2o.init(ip=Sys.getenv("H2O_HOST","localhost"), port=as.integer(Sys.getenv("H2O_PORT","54321")), strict_version_check=FALSE, startH2O=FALSE)
h2o.removeAll()

src_x = Sys.getenv("SRC_X")
src_y = Sys.getenv("SRC_Y")
X = h2o.importFile(src_x)
Y = h2o.importFile(src_y)

t = system.time(dim(hf<-h2o.merge(X, Y, method="radix")))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task="join", data="", in_rows=nrow(X), out_rows=nrow(hf), solution="h2o", fun="h2o.merge", time_sec=round(t, 3), mem_gb=round(m, 3))
h2o.rm(hf)

t = system.time(dim(hf<-h2o.merge(X, Y, method="radix")))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task="join", data="", in_rows=nrow(X), out_rows=nrow(hf), solution="h2o", fun="h2o.merge", time_sec=round(t, 3), mem_gb=round(m, 3))
h2o.rm(hf)

t = system.time(dim(hf<-h2o.merge(X, Y, method="radix")))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task="join", data="", in_rows=nrow(X), out_rows=nrow(hf), solution="h2o", fun="h2o.merge", time_sec=round(t, 3), mem_gb=round(m, 3))
h2o.rm(hf)

if( !interactive() ) q("no", status=0)
