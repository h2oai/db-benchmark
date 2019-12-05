# bash 'wc -l'
wcl = function(x) {
  as.integer(if (!file.exists(x)) NA else system(sprintf("wc -l %s | awk '{print $1}'", x), intern=TRUE))
}

# solution to file ext mapping
file.ext = function(x) {
  switch(x,
         "data.table"=, "dplyr"="R",
         "pandas"=, "cudf"=, "spark"=, "pydatatable"=, "modin"=, "dask"="py",
         "clickhouse"="sql",
         "juliadf"="jl")
}

# space separater character vector from env var
getenv = function(x) {
  v = Sys.getenv(x, NA_character_)
  if (is.na(v)) stop(sprintf("%s env var not defined.", x))
  if (nzchar(v)) {
    v = strsplit(v, " ", fixed=TRUE)[[1L]]
    if (length(v)!=length(unique(v))) stop(sprintf("%s contains non-unique values", x))
  } else v = character(0)
  v
}

# interrupt flag raise by 'touch stop'
is.sigint = function() {
  if (file.exists("stop")) {
    cat(sprintf("'stop' file detected, interrupting benchmark\n"))
    q("no")
  }
  invisible()
}

# refresh info about installed version from VERSION and REVISION files
upgraded.solution = function(x, validate=TRUE) {
  ns = gsub(".","",x,fixed=TRUE)
  f = file.path(ns, "VERSION")
  version = if (!file.exists(f)) NA_character_ else toString(readLines(f, warn=FALSE))
  f = file.path(ns, "REVISION")
  git = if (!file.exists(f)) NA_character_ else toString(readLines(f, warn=FALSE))
  if (!nzchar(git)) git = NA_character_
  ans = list(version=version, git=git)
  if (validate && (is.na(ans[["version"]]) || !nzchar(ans[["version"]]))) # for some reason it happened that one pandas run has empty char version, raise early such cases
    stop(sprintf("version of %s could not be determined based on %s/VERSION file, investigate and fix logs.csv if needed", x, ns))
  ans
}

# log runs to logs.csv so we now there was attempt to compute a script even if solution crashes before logging any timing
log_run = function(solution, task, data, action = c("start","finish","skip"), batch, nodename, stderr=NA_integer_, comment="", mockup=FALSE, verbose=TRUE) {
  stopifnot(is.character(task), length(task)==1L, !is.na(task))
  action = match.arg(action)
  timestamp=as.numeric(Sys.time())
  lg = as.data.table(c(
    list(nodename=nodename, batch=batch, solution=solution),
    upgraded.solution(solution), # list(version, git) based on VERSION and REVISION files, and extra validation so VERSION has to be always present
    list(task=task, data=data, timestamp=timestamp, action=action, stderr=stderr)
  ))
  file = "logs.csv"
  if (!mockup) fwrite(lg, file=file, append=file.exists(file), col.names=!file.exists(file))
  labels = c("start"="starting","finish"="finished","skip"="skip run")
  if (isTRUE(stderr>0L)) comment = paste0(comment, sprintf(": stderr %s", stderr))
  if (verbose) cat(sprintf("%s: %s %s %s%s\n", labels[[action]], solution, task, data, comment))
}
