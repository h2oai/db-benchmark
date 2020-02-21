write.log = function(
  timestamp=Sys.time(), # this has to be here to support timestamp provided when parsing impala or clickhouse sql logs
  task=NA_character_, data=NA_character_, in_rows=NA_integer_, question=NA_character_, out_rows=NA_integer_,
  out_cols=NA_integer_, solution=NA_character_, version=NA_character_, git=NA_character_, fun=NA_character_,
  run=NA_integer_, time_sec=NA_real_, mem_gb=NA_real_, cache=NA, chk=NA_character_, chk_time_sec=NA_real_,
  on_disk=FALSE
) {
  stopifnot(is.character(task), is.character(data), is.character(solution), is.character(fun), is.logical(on_disk))
  log.file=Sys.getenv("CSV_TIME_FILE", "time.csv")
  batch=Sys.getenv("BATCH", NA)
  nodename=toString(Sys.info()[["nodename"]])
  comment=NA_character_ # placeholder for updates to timing data
  time_sec=round(time_sec, 3)
  mem_gb=round(mem_gb, 3)
  chk_time_sec=round(chk_time_sec, 3)
  df=data.frame(nodename=nodename, batch=as.integer(batch), timestamp=as.numeric(timestamp), 
                task=task, data=data, in_rows=trunc(in_rows), question=as.character(question), out_rows=trunc(out_rows), # trunc to support big int in double
                out_cols=as.integer(out_cols), solution=solution, version=as.character(version), git=as.character(git), fun=fun,
                run=as.integer(run), time_sec=time_sec, mem_gb=mem_gb, cache=cache, chk=chk, chk_time_sec=chk_time_sec,
                comment=comment, on_disk=on_disk)
  csv_verbose = Sys.getenv("CSV_VERBOSE", "false")
  if (as.logical(csv_verbose)) cat("# ", paste(sapply(df, format, scientific=FALSE), collapse=","), "\n", sep="")
  if (!file.size(log.file)) file.remove(log.file)
  write.table(format(df, scientific=FALSE),
              file=log.file,
              append=file.exists(log.file),
              col.names=!file.exists(log.file),
              row.names=FALSE,
              quote=FALSE,
              na="",
              sep=",")
}

# short format of 1e7, 1e8 etc.
pretty_sci = function(x) {
  tmp<-strsplit(as.character(x), "+", fixed=TRUE)[[1L]]
  if(length(tmp)==1L) {
    paste0(substr(tmp, 1L, 1L), "e", nchar(tmp)-1L)
  } else if(length(tmp)==2L){
    paste0(tmp[1L], as.character(as.integer(tmp[2L])))
  }
}

# makes scalar string to store in "chk" field, check sum of arbitrary number of measures
make_chk = function(values){
  x = sapply(values, function(x) paste(format(x, scientific=FALSE), collapse="_"))
  gsub(",", "_", paste(x, collapse=";"), fixed=TRUE)
}

# bash 'ps -o rss'
memory_usage = function() {
  return(NA_real_) # disabled because during #110 system() kills the scripts
  cmd = paste("ps -o rss", Sys.getpid(), "| tail -1")
  ans = tryCatch(system(cmd, intern=TRUE, ignore.stderr=TRUE), error=function(e) NA_character_)
  as.numeric(ans) / (1024^2) # GB units
}

# join task RHS tables for LHS data name
join_to_tbls = function(data_name) {
  x_n = as.numeric(strsplit(data_name, "_", fixed=TRUE)[[1L]][2L])
  y_n = setNames(c(x_n/1e6, x_n/1e3, x_n), c("small","medium","big"))
  sapply(sapply(y_n, pretty_sci), gsub, pattern="NA", x=data_name)
}
