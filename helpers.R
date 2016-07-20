write.log = function(
  timestamp=Sys.time(), # this has to be here to support timestamp provided when parsing impala sql logs
  task=NA_character_, data=NA_character_, in_rows=NA_integer_, out_rows=NA_integer_,
  solution=NA_character_, fun=NA_character_, run=NA_integer_, time_sec=NA_real_, mem_gb=NA_real_
) {
  stopifnot(is.character(task), is.character(data), is.character(solution), is.character(fun))
  log.file=Sys.getenv("CSV_TIME_FILE", "time.csv")
  batch=Sys.getenv("BATCH", NA)
  comment="" # placeholder for updates to timing data
  time_sec=round(time_sec, 3)
  mem_gb=round(mem_gb, 3)
  df=data.frame(batch=as.integer(batch), timestamp=as.numeric(timestamp), 
                task=task, data=data, in_rows=as.integer(in_rows), out_rows=as.integer(out_rows),
                solution=solution, fun=fun, run=as.integer(run), time_sec=time_sec, mem_gb=mem_gb,
                comment=comment)
  cat("# ", paste(sapply(df, toString), collapse=","), "\n", sep="")
  write.table(df,
              file=log.file,
              append=file.exists(log.file),
              col.names=!file.exists(log.file),
              row.names=FALSE,
              quote=FALSE,
              na="",
              sep=",")
}

# extract dataset volume from SRC_X and SRC_Y env vars for join tests, specific to our source filenames
get.nrow = function(x) {
  if(any(is.na(x))) stop("env vars SRC_X and SRC_Y must be defined, see run.conf")
  # get total sum of row count from X and Y
  Reduce("+", as.integer(substr(tmp<-sapply(strsplit(sapply(strsplit(x, "/", fixed=TRUE), function(x) x[length(x)]), "_", fixed=TRUE), `[`, 1L), 2L, nchar(tmp))))
}
