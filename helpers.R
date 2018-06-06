write.log = function(
  timestamp=Sys.time(), # this has to be here to support timestamp provided when parsing impala sql logs
  task=NA_character_, data=NA_character_, in_rows=NA_integer_, question=NA_character_, out_rows=NA_integer_,
  out_cols=NA_integer_, solution=NA_character_, version=NA_character_, git=NA_character_, fun=NA_character_,
  run=NA_integer_, time_sec=NA_real_, mem_gb=NA_real_, cache=NA, chk=NA_character_, chk_time_sec=NA_real_
) {
  stopifnot(is.character(task), is.character(data), is.character(solution), is.character(fun))
  log.file=Sys.getenv("CSV_TIME_FILE", "time.csv")
  batch=Sys.getenv("BATCH", NA)
  comment=NA_character_ # placeholder for updates to timing data
  time_sec=round(time_sec, 3)
  mem_gb=round(mem_gb, 3)
  chk_time_sec=round(chk_time_sec, 3)
  df=data.frame(batch=as.integer(batch), timestamp=as.numeric(timestamp), 
                task=task, data=data, in_rows=trunc(in_rows), question=as.character(question), out_rows=trunc(out_rows), # trunc to support big int in double
                out_cols=as.integer(out_cols), solution=solution, version=as.character(version), git=as.character(git), fun=fun,
                run=as.integer(run), time_sec=time_sec, mem_gb=mem_gb, cache=cache, chk=chk, chk_time_sec=chk_time_sec,
                comment=comment)
  csv_verbose = Sys.getenv("CSV_VERBOSE", "true")
  if (as.logical(csv_verbose)) cat("# ", paste(sapply(df, format, scientific=FALSE), collapse=","), "\n", sep="")
  write.table(format(df, scientific=FALSE),
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
  if(any(is.na(x) | nchar(x)==0L)) stop("env vars SRC_X and SRC_Y must be defined, see run.conf")
  # get total sum of row count from X and Y
  Reduce("+", as.integer(substr(tmp<-sapply(strsplit(sapply(strsplit(x, "/", fixed=TRUE), function(x) x[length(x)]), "_", fixed=TRUE), `[`, 1L), 2L, nchar(tmp))))
}

# data integration: bing new logs with more fields, expand history for new cols
bind.logs = function(history, new) {
  stopifnot(requireNamespace("data.table"), requireNamespace("bit64"), file.exists(history), file.exists(new))
  history_dt = read_timing(history, raw=TRUE)
  new_dt = read_timing(new, raw=TRUE)
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

# short format of 1e7, 1e8 etc.
pretty_sci = function(x) {
  tmp<-strsplit(as.character(x), "+", fixed=TRUE)[[1L]]
  if(length(tmp)==1L) {
    paste0(substr(tmp, 1L, 1L), "e", nchar(tmp)-1L)
  } else if(length(tmp)==2L){
    paste0(tmp[1L], as.character(as.integer(tmp[2L])))
  }
}

# elegant way to read timing logs
read_timing = function(csv.file=Sys.getenv("CSV_TIME_FILE", "~/time.csv"), raw=FALSE, cols=c("presto"=2L,"data.table"=3L,"dplyr"=4L,"h2o"=5L,"impala"=6L,"pandas"=7L,"spark"=8L,"dask"=9L)) {
  if (!file.exists(csv.file)) stop(sprintf("File %s storing timings does not exists. Did you successfully run.sh benchmark?", csv.file))
  dt = fread(csv.file, na.strings=c("","NA","NaN"), sep=",", colClasses=c(batch="integer", in_rows="integer64", out_rows="integer64", comment="character", version="character", git="character", question="character", data="character", cache="logical", chk="character", chk_time_sec="numeric"))
  if (raw) return(dt)
  # assign colors to solutions
  dt[, col := cols[solution], .(solution)]
  # print variable
  dt[, datetime := as.POSIXct(timestamp, origin="1970-01-01")]
  dt[]
}

# wrapper on read_timing with filter on latest benchmark batch for each query
last_timing = function(csv.file=Sys.getenv("CSV_TIME_FILE", "~/time.csv"), x) {
  if (missing(x)) x = read_timing(csv.file=csv.file) else stopifnot(is.data.table(x))
  x[order(timestamp), .SD[.N], by=.(task, data, in_rows, question, solution, fun, run, cache) # take recent only including cache granularity
    ][, .SD[1L], by=.(task, data, in_rows, question, solution, run) # use .SD[.N] for cache=TRUE if needed
      ]
}

# makes scalar string to store in "chk" field, check sum of arbitrary number of measures
make_chk = function(values){
  x = sapply(values, function(x) paste(format(x, scientific=FALSE), collapse="_"))
  gsub(",", "_", paste(x, collapse=";"), fixed=TRUE)
}

# data.table print options, just for formatting when viewing logs interactively
ppc = function(trunc.char) options(datatable.prettyprint.char=trunc.char)

# report semi manually maintained metadata

file.ext = function(x)
  switch(x,
         "data.table"=, "dplyr"=, "h2o"="R",
         "pandas"=, "pydatatable"=, "dask"="py",
         "impala"=, "presto"="sql",
         "spark"="scala")

solution.date = function(solution, version, git, only.date=FALSE, use.cache=FALSE) {
  stopifnot(is.character(solution), !is.na(version) | !is.na(git))
  if (use.cache) cache <- if(exists(cache.obj<-".solution.date.cache", envir=.GlobalEnv)) get(cache.obj, envir=.GlobalEnv) else list()
  gh_repos = c("h2o"="h2oai/h2o-3",
               "impala"="cloudera/Impala",
               "data.table"="Rdatatable/data.table",
               "pydatatable"="h2oai/datatable"
               #"dask"="dask/dask" # since next version: https://github.com/dask/dask/pull/1760/commits/a0fd57285cb980b1b297221cf2550cd0a4289f42
               )
  solution_versions = list(
    spark = c("2.0.0" = "2016-07-19",
              "2.1.0-SNAPSHOT" = "2016-08-15",
              "2.0.1" = "2016-10-03"),
    presto = c("0.150" = "2016-07-07"),
    pandas = c("0.18.1" = "2016-05-03",
               "0.19.0" = "2016-10-02",
               "0.19.1" = "2016-11-03",
               "0.23.0" = "2018-05-16"),
    dask = c("0.10.2" = "2016-07-26",
             "0.11.0" = "2016-08-18",
             "0.11.1" = "2016-10-07"),
    dplyr = c("0.5.0" = "2016-06-23",
              "0.7.5" = "2018-05-19")
  )
  if (!is.na(git)) {
    if (use.cache && !is.null(cgit<-cache[[solution]][[git]])) {
      #message("using git commit date from cache instead of github api")
      r <- cgit
    } else {
      string = tryCatch(jsonlite::fromJSON(
        sprintf("https://api.github.com/repos/%s/git/commits/%s",
                gh_repos[[solution]], git)
      )[["committer"]][["date"]], error=function(e) NA_character_) # error when hit github api limit
      r = strptime(string, "%F")[1L]
      if (use.cache) {
        if (!is.na(r)) cache[[solution]][[git]] <- r
        #message("writing git commit date to cache")
        assign(cache.obj, cache, envir=.GlobalEnv)
      }
    }
  } else if (!is.na(version)) {
    if (!version %in% names(solution_versions[[solution]])) {
      warning(sprintf("Solution %s in version %s doesn't have date defined to corresponding version. See helpers.R solution.date function to add version date.", solution, version))
      r = NA_character_
    } else {
      r = as.character(solution_versions[[solution]][[version]])[1L]
    }
  } else {
    stop("solution.date lookup requires non-NA git hash or version")
  }
  if (!only.date) {
    sprintf("%s (%s)", version, as.character(as.Date(r))) # get rid of tz, etc.
  } else {
    as.character(as.Date(r))
  }
}

memory_usage = function() {
  as.numeric(system(paste("ps -o rss", Sys.getpid(), "| tail -1"), intern=TRUE)) / (1024^2)
}
