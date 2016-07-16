#!/usr/bin/env Rscript

cat("# join-data.table.R\n")

memory_usage = function() {
  as.numeric(system(paste("ps -o rss", Sys.getpid(), "| tail -1"), intern=TRUE)) / (1024^2)
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


src_x = Sys.getenv("SRC_X", NA_character_)
src_y = Sys.getenv("SRC_Y", NA_character_)
get.nrow = function(x) {
  if(any(is.na(x))) stop("env vars SRC_X and SRC_Y must be defined, see join.sh")
  # get total sum of row count from X and Y
  Reduce("+", as.integer(substr(tmp<-sapply(strsplit(sapply(strsplit(x, "/", fixed=TRUE), function(x) x[length(x)]), "_", fixed=TRUE), `[`, 1L), 2L, nchar(tmp))))
}
if (get.nrow(c(src_x,src_y)) > 2e9L) {
  cat("# join with data.table skipped due data volume cap for single machine set to total 2e9 rows")
  quit("no", status=0) # datasets > 1e9 too big to try load on single machine
}

library(data.table)
X = fread(if(file.exists(src_x)) src_x else sprintf("hadoop fs -cat %s", src_x)) # csv can be provided in local dir for faster import
Y = fread(if(file.exists(src_y)) src_y else sprintf("hadoop fs -cat %s", src_y))

t = system.time(dim(dt<-X[Y, on="KEY", nomatch=0L]))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task="join", data="", in_rows=nrow(X), out_rows=nrow(dt), solution="data.table", fun="[.data.table", time_sec=round(t, 3), mem_gb=round(m, 3))
rm(dt)

t = system.time(dim(dt<-X[Y, on="KEY", nomatch=0L]))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task="join", data="", in_rows=nrow(X), out_rows=nrow(dt), solution="data.table", fun="[.data.table", time_sec=round(t, 3), mem_gb=round(m, 3))
rm(dt)

t = system.time(dim(dt<-X[Y, on="KEY", nomatch=0L]))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task="join", data="", in_rows=nrow(X), out_rows=nrow(dt), solution="data.table", fun="[.data.table", time_sec=round(t, 3), mem_gb=round(m, 3))
rm(dt)

if( !interactive() ) q("no", status=0)
