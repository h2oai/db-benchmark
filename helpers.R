write.log = function(
  timestamp=Sys.time(), # this has to be here to support timestamp provided when parsing impala sql logs
  task=NA_character_, data=NA_character_, in_rows=NA_integer_, question=NA_character_, out_rows=NA_integer_,
  solution=NA_character_, version=NA_character_, git=NA_character_, fun=NA_character_, run=NA_integer_, time_sec=NA_real_, mem_gb=NA_real_
) {
  stopifnot(is.character(task), is.character(data), is.character(solution), is.character(fun))
  log.file=Sys.getenv("CSV_TIME_FILE", "time.csv")
  batch=Sys.getenv("BATCH", NA)
  comment="" # placeholder for updates to timing data
  time_sec=round(time_sec, 3)
  mem_gb=round(mem_gb, 3)
  df=data.frame(batch=as.integer(batch), timestamp=as.numeric(timestamp), 
                task=task, data=data, in_rows=trunc(in_rows), question=as.character(question), out_rows=trunc(out_rows), # trunc to support big int in double
                solution=solution, version=as.character(version), git=as.character(git), fun=fun, run=as.integer(run), time_sec=time_sec, mem_gb=mem_gb,
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

# data integration: bing new logs with more fields, expand history for new cols
bind.logs = function(history, new) {
  stopifnot(requireNamespace("data.table"), file.exists(history), file.exists(new))
  na.strings=c("","NA","NaN")
  sep=","
  colClasses=c(batch="integer", comment="character", version="character", git="character")
  history_dt = data.table::fread(history, na.strings=na.strings, sep=sep, colClasses=colClasses)
  new_dt = data.table::fread(new, na.strings=na.strings, sep=sep, colClasses=colClasses)
  setdiff(names(new_dt), names(history_dt))
  stopifnot(nrow(new_dt) > 0L, nrow(history_dt) > 0L)
  if(length(in_hist_only<-setdiff(names(history_dt), names(new_dt)))) stop(sprintf("Could not extend logs history. Following columns are present in history data but not in new: %s", paste(in_hist_only, collapse=",")))
  ans = data.table::rbindlist(list(history_dt, new_dt), use.names=TRUE, fill=TRUE)
  backup.file = sprintf("%s_%s.csv", gsub(".csv", "", history, fixed=TRUE), format(Sys.time(), "%Y%m%d_%H%M%S"))
  file.rename(history, backup.file)
  data.table::setcolorder(ans, names(new_dt))
  write.table(ans,
              file=history,
              col.names=TRUE,
              row.names=FALSE,
              quote=FALSE,
              na="",
              sep=",")
  invisible(ans)
}

pretty_sci = function(x) {
  tmp<-strsplit(as.character(x), "+", fixed=TRUE)[[1L]]
  if(length(tmp)==1L) {
    paste0(substr(tmp, 1L, 1L), "e", nchar(tmp)-1L)
  } else if(length(tmp)==2L){
    paste0(tmp[1L], as.character(as.integer(tmp[2L])))
  }
}
