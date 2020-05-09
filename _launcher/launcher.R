# bash 'wc -l'
wcl = function(x) {
  as.integer(if (!file.exists(x)) NA else system(sprintf("wc -l %s | awk '{print $1}'", x), intern=TRUE))
}
readret = function(x) {
  if (file.exists(x) && file.size(x)) {
    ret = readLines(x)
    if (length(ret)!=1L) stop(sprintf("'%s' file containts more than a single line"))
    ret
  } else NA_character_
}

# solution to file ext mapping
file.ext = function(x) {
  ans = switch(
    x,
    "data.table"=, "dplyr"="R",
    "pandas"=, "cudf"=, "spark"=, "pydatatable"=, "modin"=, "dask"="py",
    "clickhouse"="sql",
    "juliadf"="jl"
  )
  if (is.null(ans)) stop(sprintf("solution %s does not have file extension defined in file.ext helper function", x))
  ans
}

# no dots solution name used in paths
solution.path = function(x) {
  gsub(".", "", x, fixed=TRUE)
}

# python virtual env or conda env activate command prefix
solution.venv = function(x) {
  ext = file.ext(x)
  if (ext=="py") { # https://stackoverflow.com/questions/52779016/conda-command-working-in-command-prompt-but-not-in-bash-script
    ns = solution.path(x)
    if (x%in%c("cudf")) sprintf("source ~/anaconda3/etc/profile.d/conda.sh && conda activate %s && ", ns)
    else sprintf("source ./%s/py-%s/bin/activate && ", ns, ns)
  } else ""
}

# construct solution.R cmd call
solution.cmd = function(s, t, d) {
  dd = strsplit(d, "_", fixed=TRUE)[[1L]]
  if (length(dd)!=5L)
    stop("data_name is expected to have exactly four underscore characters, like G1_1e7_1e2_0_0 or J1_1e7_NA_0_0")
  names(dd) = c("prefix","nrow","k","na","sort")
  sprintf("./_launcher/solution.R --solution=%s --task=%s --nrow=%s --k=%s --na=%s --sort=%s --out=%s",
          s, t, dd[["nrow"]], dd[["k"]], dd["na"], dd["sort"],
          Sys.getenv("CSV_TIME_FILE","time.csv"))
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
is.stop = function() {
  if (file.exists("stop")) {
    cat(sprintf("'stop' file detected: interrupting benchmark at %s\n", format(Sys.time(), usetz=TRUE)))
    q("no")
  }
  invisible()
}
# pause flag raises by 'touch pause'
is.pause = function() {
  if (file.exists("pause")) {
    cat(sprintf("'pause' file detected: pausing  benchmark at %s\n", format(Sys.time(), usetz=TRUE)))
    while(file.exists("pause")) Sys.sleep(60) # check every minute
    cat(sprintf("'pause' file absent:   resuming benchmark at %s\n", format(Sys.time(), usetz=TRUE)))
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

# detect if script has been already run before for currently installed version/revision
lookup_run_batch = function(dt) {
  .nodename = unique(dt$nodename)
  stopifnot(length(.nodename)==1L)
  forcerun = as.logical(Sys.getenv("FORCE_RUN", "false"))
  logs.csv = Sys.getenv("CSV_LOGS_FILE","logs.csv")
  time.csv = Sys.getenv("CSV_TIME_FILE","time.csv")
  if (!forcerun && file.exists(time.csv) && file.exists(logs.csv) && nrow(timings<-fread(time.csv)[nodename==.nodename]) && nrow(logs<-fread(logs.csv)[nodename==.nodename])) {
    timings[, .N,, c("nodename","batch","solution","task","data","version","git")
            ][, "N" := NULL
              ][!nzchar(git), "git" := NA_character_
                ][] -> timings
    logs[action!="skip", .N,, c("nodename","batch","solution","task","data","version","git")
         ][N==2L # filter out not yet finished, might be still running or fatallly crashed
           ][, "N" := NULL
             ][!nzchar(git), "git" := NA_character_
               ][] -> logs
    # there might be no timings for solutions that crashed, thus join to logs to still have those NA timings
    past = timings[logs, .(nodename, batch, solution, task, data, timing_version=x.version, timing_git=x.git, logs_version=i.version, logs_git=i.git), on=c("nodename","batch","solution","task","data")]
    # NA timing_version/git is when solution crashed
    # NA logs_version/git is when VERSION/REVISION files where not created but it is already part of run.sh
    # rules for running/skipping:
    # 1. compare to most recent run only per expected granularity
    past[, "recent_batch":=max(batch, na.rm=TRUE), by=c("nodename","solution","task","data")]
    recent = past[batch==recent_batch][, c("recent_batch") := NULL][]
    # 2. where possible compare on git revision, otherwise version
    recent[, "compare" := logs_git][is.na(compare), "compare" := logs_version]
    upgraded = rbindlist(sapply(unique(dt$solution), upgraded.solution, simplify=FALSE), idcol="solution")
    upgraded[, "compare" := git][is.na(compare), "compare" := version]
    recent[, c("timing_version","timing_git","logs_version","logs_git") := NULL] # remove unused
    if (any(recent[, .N>1L, by=c("nodename","solution","task","data")]$V1))
      stop("Recent timings and logs produces more rows than expected, investigate")
    dt[upgraded, "compare" := i.compare, on="solution"]
    dt[recent, "run_batch" := i.batch, on=c("nodename","solution","task","data","compare")]
  } else {
    dt[, c("compare","run_batch") := list(NA_character_, NA_integer_)]
  }
}

# log runs to logs.csv so we now there was attempt to compute a script even if solution crashes before logging any timing
log_run = function(solution, task, data, action = c("start","finish","skip"), batch, nodename, ret=NA_character_, stderr=NA_integer_, comment="", mockup=FALSE, verbose=TRUE) {
  stopifnot(is.character(task), length(task)==1L, !is.na(task), is.character(ret), length(ret)==1L)
  action = match.arg(action)
  timestamp=as.numeric(Sys.time())
  lg = as.data.table(c(
    list(nodename=nodename, batch=batch, solution=solution),
    upgraded.solution(solution), # list(version, git) based on VERSION and REVISION files, and extra validation so VERSION has to be always present
    list(task=task, data=data, timestamp=timestamp, action=action, stderr=stderr, ret=ret)
  ))
  logs.csv = Sys.getenv("CSV_LOGS_FILE","logs.csv")
  if (!mockup) fwrite(lg, file=logs.csv, append=file.exists(logs.csv), col.names=!file.exists(logs.csv))
  labels = c("start" ="start: ",
             "finish"="finish:",
             "skip"  ="skip:  ") # to align console output
  status = ""
  if (!is.na(ret)) status = sprintf(": %s", ret)
  if (isTRUE(stderr>0L)) status = sprintf("%s: stderr %s", status, stderr)
  if (nzchar(comment)) status = sprintf("%s: %s", status, comment)
  if (verbose) cat(sprintf("%s %s %s %s%s\n", labels[[action]], solution, task, data, status))
}

# main launcher than loops over solutions, tasks, data and when need run it in new shell command
launch = function(dt, mockup, out_dir="out") {
  stopifnot(
    is.data.table(dt), dir.exists(out_dir),
    c("compare","run_batch")%in%names(dt), # ensure lookup_run_batch was called on dt
    uniqueN(dt$nodename)==1L, # this should be single value of current nodename
    !anyNA(dt$solution), !anyNA(dt$task), !anyNA(dt$data)
  )
  batch = Sys.getenv("BATCH", NA)
  .nodename = unique(dt$nodename)
  ## solution
  solutions = dt[, unique(solution)]
  for (s in solutions) { # s = solutions[1]
    ns = solution.path(s)
    ext = file.ext(s)
    venv = solution.venv(s)
    ### task
    tasks = dt[.(s), unique(task), on="solution"]
    for (t in tasks) { # t = tasks[1]
      #### data
      data = dt[.(s, t), data, on=c("solution","task")]
      for (d in data) { # d = data[1]
        is.stop() # interrupt using 'stop' file #74
        is.pause() # pause using 'pause' file #143
        this_run = dt[.(s, t, d), on=c("solution","task","data")]
        if (nrow(this_run) != 1L) stop(sprintf("single run for %s-%s-%s has %s entries while it must have exactly one", s, t, d, nrow(this_run)))
        out_file = sprintf("%s/run_%s_%s_%s.out", out_dir, ns, t, d)
        err_file = sprintf("%s/run_%s_%s_%s.err", out_dir, ns, t, d)
        ret_file = sprintf("%s/run_%s_%s_%s.ret", out_dir, ns, t, d)
        if (!is.na(this_run$run_batch)) {
          comment = sprintf("%s run on %s", substr(this_run$compare, 1L, 7L), format(as.Date(as.POSIXct(this_run$run_batch, origin="1970-01-01")), "%Y%m%d"))
          log_run(s, t, d, action="skip", batch=batch, nodename=.nodename, ret=readret(ret_file), stderr=wcl(err_file), comment=comment, mockup=mockup) # action 'skip' also logs number of stderr lines from previos run and previous exit code
          next
        }
        log_run(s, t, d, action="start", batch=batch, nodename=.nodename, mockup=mockup)
        if (!mockup) {
          if (file.exists(out_file)) file.remove(out_file)
          if (file.exists(err_file)) file.remove(err_file)
          if (file.exists(ret_file)) file.remove(ret_file)
        }
        cmd = sprintf("%s > %s 2> %s", solution.cmd(s, t, d), out_file, err_file) # ./_launcher/solution.R ... > out 2> err
        shcmd = sprintf("/bin/bash -c \"%s%s\"", venv, cmd) # this is needed to source python venv
        if (!mockup) {
          warn = NULL
          p = proc.time()[[3L]]
          tryCatch(
            ret <- system(shcmd, timeout=this_run$timeout_s), # here script actually runs
            warning = function(w) { # R's system warnings like 'timed out', they are not yet in stderr as all the inner ones
              cat(paste0(w[["message"]],"\n"), file=err_file, append=TRUE)
              warn <<- w[["message"]]
            }
          )
          if (length(warn) && ret==0L)
            cat(sprintf("command '%s' timed out(?) but still exited with 0 code, timeout %ds, took %ds, warning '%s'\n", shcmd, this_run$timeout_s, proc.time()[[3L]]-p, warn), file="timeout-exit-codes.out", append=TRUE)
          cat(paste0(ret,"\n"), file=ret_file, append=FALSE)
        }
        log_run(s, t, d, action="finish", batch=batch, nodename=.nodename, ret=readret(ret_file), stderr=wcl(err_file), mockup=mockup)
      }
    }
  }
  invisible()
}
