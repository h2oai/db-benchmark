# bash 'wc -l'
wcl = function(x) {
  as.integer(if (!file.exists(x)) NA else system(sprintf("wc -l %s | awk '{print $1}'", x), intern=TRUE))
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

# data_name env var for each task
task.env = function(x) {
  ans = switch(
    x,
    "groupby"="SRC_GRP_LOCAL",
    "join"="SRC_JN_LOCAL"
  )
  if (is.null(ans)) stop(sprintf("task %s does not have data name environment variable defined in task.env helper function", x))
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

# sql scripts are using extra exec shell script, related only to clickhouse as of now
solution.cmd = function(s, t, d) {
  ext = file.ext(s)
  if (ext=="sql") {
    sprintf("exec.sh %s %s", t, d)
  } else sprintf("%s-%s.%s", t, solution.path(s), ext)
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

# dynamic LHS in: Sys.setenv(var = value)
setenv = function(var, value, quiet=TRUE) {
  stopifnot(is.character(var), !is.na(var), length(value)==1L, is.atomic(value))
  qc = as.call(c(list(quote(Sys.setenv)), setNames(list(value), var)))
  if (!quiet) print(qc)
  eval(qc)
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

# detect if script has been already run before for currently installed version/revision
lookup_run_batch = function(dt, .nodename) {
  forcerun = as.logical(Sys.getenv("FORCE_RUN", "false"))
  if (!forcerun && file.exists("time.csv") && file.exists("logs.csv") && nrow(timings<-fread("time.csv")[nodename==.nodename]) && nrow(logs<-fread("logs.csv")[nodename==.nodename])) {
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

# main launcher than loops over solutions, tasks, data and when need run it in new shell command
launch = function(dt, mockup, out_dir="out") {
  stopifnot(
    is.data.table(dt), dir.exists(out_dir),
    c("compare","run_batch")%in%names(dt), # ensure lookup_run_batch was called on dt
    uniqueN(dt$nodename)==1L # this should be single value of current nodename
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
      data_name_env = task.env(t)
      #### data
      data = dt[.(s, t), data, on=c("solution","task")]
      for (d in data) { # d = data[1]
        is.sigint() # interrupt using 'stop' file #74
        this_run = dt[.(s, t, d), on=c("solution","task","data")]
        if (nrow(this_run) != 1L) stop(sprintf("single run for %s-%s-%s has %s entries while it must have exactly one", s, t, d, nrow(this_run)))
        out_file = sprintf("%s/run_%s_%s_%s.out", out_dir, ns, t, d)
        err_file = sprintf("%s/run_%s_%s_%s.err", out_dir, ns, t, d)
        if (!is.na(this_run$run_batch)) {
          comment = sprintf(": %s run on %s", substr(this_run$compare, 1L, 7L), format(as.Date(as.POSIXct(this_run$run_batch, origin="1970-01-01")), "%Y%m%d"))
          log_run(s, t, d, action="skip", batch=batch, nodename=.nodename, stderr=wcl(err_file), comment=comment, mockup=mockup) # action 'skip' also logs number of stderr lines from previos run
          next
        }
        log_run(s, t, d, action="start", batch=batch, nodename=.nodename, mockup=mockup)
        setenv(data_name_env, d)
        if (!mockup) {
          if (file.exists(out_file)) file.remove(out_file)
          if (file.exists(err_file)) file.remove(err_file)
        }
        localcmd = solution.cmd(s, t, d)
        cmd = sprintf("./%s/%s > %s 2> %s", ns, localcmd, out_file, err_file)
        shcmd = sprintf("/bin/bash -c \"%s%s\"", venv, cmd) # this is needed to source venv
        if (!mockup) {
          tryCatch(
            system(shcmd, timeout=this_run$timeout_s), # here script actually runs
            warning = function(w) {
              # this is to catch and log timeout but we want every warning to be written to stderr
              if (grepl("timed out", w[["message"]], fixed=TRUE)) {
                # input NA timings? would require to push up 'question' factor here but would simplify(?) exception handling on benchplot
              }
              cat(paste0(w[["message"]],"\n"), file=err_file, append=TRUE)
            }
          )
        }
        Sys.unsetenv(data_name_env)
        log_run(s, t, d, action="finish", batch=batch, nodename=.nodename, stderr=wcl(err_file), mockup=mockup)
      }
    }
  }
  invisible()
}
