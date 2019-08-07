library(data.table)
source("helpers.R")

is.sigint()

batch = Sys.getenv("BATCH", NA)
nodename = Sys.info()[["nodename"]]
mockup = as.logical(Sys.getenv("MOCKUP", "false"))
forcerun = as.logical(Sys.getenv("FORCE_RUN", "false"))

if (packageVersion("data.table") <= "1.12.0") stop("db-benchmark launcher script depends on recent data.table features, install at least 1.12.0. If you need to benchmark older data.table tweak script to use custom library where older version is installed.")

log_run = function(solution, task, data, action = c("start","finish","skip"), batch, nodename, stderr=NA_integer_, comment="", mockup=FALSE, verbose=TRUE) {
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
run_tasks = getenv("RUN_TASKS") #run_tasks = "groupby"
if (!length(run_tasks)) q("no")
run_solutions = getenv("RUN_SOLUTIONS") #run_solutions=c("data.table","dplyr","pydatatable","spark","pandas")
if (!length(run_solutions)) q("no")

data = fread("data.csv")
data = data[active==TRUE, # filter on active datasets
            ][run_tasks, on="task", nomatch=NULL # filter for env var RUN_TASKS
              ][, c("active") := NULL # remove unused, id+seq to be used for join
                ][]

timeout = fread("timeout.csv")
timeout = timeout[run_tasks, on="task", nomatch=NULL] # filter for env var RUN_TASKS
stopifnot(nrow(timeout)==1L)

solution = rbindlist(list(
  dask = list(task=c("groupby","join")),
  data.table = list(task=c("groupby","join")),
  dplyr = list(task=c("groupby","join")),
  juliadf = list(task=c("groupby","join")),
  modin = list(task=c()),
  pandas = list(task=c("groupby","join")),
  pydatatable = list(task=c("groupby")), # join after https://github.com/h2oai/datatable/issues/1080
  spark = list(task=c("groupby","join")),
  clickhouse = list(task=c("groupby")),
  cudf = list(task=c("groupby"))
), idcol="solution")
solution = solution[run_solutions, on="solution", nomatch=NULL] # filter for env var RUN_SOLUTIONS
stopifnot(nrow(solution) > 0L) # when added new solution and forget to add here this will catch

# what to run
dt = solution[data, on="task", allow.cartesian=TRUE]

# G2 grouping data only relevant for clickhouse
dt = dt[!(substr(data, 1L, 2L)=="G2" & solution!="clickhouse")]

# log current machine name
dt[, "nodename" := nodename]

# filter runs to only what is new
.nodename = nodename
if (!forcerun && file.exists("time.csv") && file.exists("logs.csv") && nrow(timings<-fread("time.csv")[nodename==.nodename]) && nrow(logs<-fread("logs.csv")[nodename==.nodename])) {
  timings[, .N,, c("nodename","batch","solution","task","data","version","git")
          ][, "N" := NULL
            ][!nzchar(git), "git" := NA_character_
              ][] -> timings
  logs[action!="skip", .N,, c("nodename","batch","solution","task","data","version","git")
       ][N==2L
         ][, "N" := NULL
           ][!nzchar(git), "git" := NA_character_
             ][] -> logs
  past = timings[logs, .(nodename, batch, solution, task, data, timing_version=x.version, timing_git=x.git, logs_version=i.version, logs_git=i.git), on=c("nodename","batch","solution","task","data")] # there might be no timings for solutions that crashed, thus join to logs
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

# run

## solution
solutions = dt[, unique(solution)]
for (s in solutions) { #s = solutions[1]
  ### task
  tasks = dt[.(s), unique(task), on="solution"]
  for (t in tasks) { #t = tasks[1]
    #### data
    data = dt[.(s, t), data, on=c("solution","task")]
    for (d in data) { #d=data[1]
      is.sigint() # interrupt using 'stop' file #74
      this_run = dt[.(s, t, d), on=c("solution","task","data")]
      if (nrow(this_run) != 1L)
        stop(sprintf("single run for %s-%s-%s has %s entries while it must have exactly one", s, t, d, nrow(this_run)))
      ns = gsub(".", "", s, fixed=TRUE)
      out_dir = "out"
      out_file = sprintf("%s/run_%s_%s_%s.out", out_dir, ns, t, d)
      err_file = sprintf("%s/run_%s_%s_%s.err", out_dir, ns, t, d)
      if (!is.na(this_run$run_batch)) {
        comment = sprintf(": %s run on %s", substr(this_run$compare, 1, 7), format(as.Date(as.POSIXct(this_run$run_batch, origin="1970-01-01")), "%Y%m%d"))
        log_run(s, t, d, action="skip", batch=batch, nodename=nodename, stderr=wcl(err_file), comment=comment, mockup=mockup) # skip also logs number of lines stderr from previos run
        next
      }
      log_run(s, t, d, action="start", batch=batch, nodename=nodename, mockup=mockup)
      if (t=="groupby") {
        Sys.setenv("SRC_GRP_LOCAL"=d)
      } else if (t=="join") {
        Sys.setenv("SRC_JN_LOCAL"=d)
      } else stop("unknown task in launcher.R script")
      if (!mockup) {
        if (file.exists(out_file)) file.remove(out_file)
        if (file.exists(err_file)) file.remove(err_file)
      }
      ext = file.ext(s)
      cmd = if (ext=="sql") { # only clickhouse for now
        sprintf("./%s-exec.sh %s %s > %s 2> %s", ns, t, d, out_file, err_file)
      } else sprintf("./%s/%s-%s.%s > %s 2> %s", ns, t, ns, ext, out_file, err_file)
      venv = if (ext=="py") {
        if (ns%in%c("cudf")) sprintf("conda activate %s && ", ns)
        else sprintf("source ./%s/py-%s/bin/activate && ", ns, ns)
      } else ""
      shcmd = sprintf("/bin/bash -c \"%s%s\"", venv, cmd)
      timeout_s = 60*timeout[["minutes"]] # see timeout.csv
      if (!mockup) {
        tryCatch(
          system(shcmd, timeout=timeout_s), # here script actually runs
          warning = function(w) {
            # this is to catch and log timeout but we want every warning to be written to stderr
            if (grepl("timed out", w[["message"]], fixed=TRUE)) {
              # input NA timings? would require to push up 'question' factor here but would simplify(?) exception handling on benchplot
            }
            cat(paste0(w[["message"]],"\n"), file=err_file, append=TRUE)
          }
        )
      }
      if (t=="groupby") {
        Sys.unsetenv("SRC_GRP_LOCAL")
      } else if (t=="join") {
        Sys.unsetenv("SRC_JN_LOCAL")
      }
      log_run(s, t, d, action="finish", batch=batch, nodename=nodename, stderr=wcl(err_file), mockup=mockup)
    }
  }
}
