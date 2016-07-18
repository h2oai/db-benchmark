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
