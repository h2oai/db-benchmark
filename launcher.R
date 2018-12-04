library(data.table)

batch = Sys.getenv("BATCH", NA)
nodename = Sys.info()[["nodename"]]

upgraded.solution = function(x) {
  ns = gsub(".","",x,fixed=TRUE)
  f = file.path(ns, "VERSION")
  version = if (!file.exists(f)) NA_character_ else toString(readLines(f, warn=FALSE))
  f = file.path(ns, "REVISION")
  git = if (!file.exists(f)) NA_character_ else toString(readLines(f, warn=FALSE))
  if (!nzchar(git)) git = NA_character_
  list(version=version, git=git)
}
wcl = function(x) {
  as.integer(if (!file.exists(x)) NA else system(sprintf("wc -l %s | awk '{print $1}'", x), intern=TRUE))
}
log_run = function(solution, task, data, action = c("start","finish","skip"), batch, nodename, stderr=NA_integer_, comment="", verbose=TRUE) {
  action = match.arg(action)
  if (!missing(this_run)) stopifnot(is.data.table(this_run))
  timestamp=as.numeric(Sys.time())
  lg = as.data.table(c(list(nodename=nodename, batch=batch, solution=solution), upgraded.solution(solution), list(task=task, data=data, timestamp=timestamp, action=action)))
  file = "logs.csv"
  fwrite(lg, file=file, append=file.exists(file), col.names=!file.exists(file))
  labels = c("start"="starting","finish"="finished","skip"="skip run")
  if (!sTRUE(stderr>0L)) comment = paste0(comment, sprintf(": stderr %s", stderr))
  if (verbose) cat(sprintf("%s %s %s %s%s\n", labels[[action]], solution, task, data, comment))
}
file.ext = function(x) {
  switch(x,
         "data.table"=, "dplyr"="R",
         "pandas"=, "spark"=, "pydatatable"=, "modin"=, "dask"="py",
         "juliadf"="jl")
}
getenv = function(x) {
  v = Sys.getenv(x, NA_character_)
  if (is.na(v)) stop(sprintf("%s env var not defined.", x))
  v = strsplit(v, " ", fixed=TRUE)[[1L]]
  if (length(v)!=length(unique(v))) stop(sprintf("%s contains non-unique values", x))
  v
}
run_tasks = getenv("RUN_TASKS") #run_tasks = "groupby"
run_solutions = getenv("RUN_SOLUTIONS") #run_solutions=c("data.table","dplyr","pydatatable","spark","pandas")

data = fread("data.csv")
data = data[active==TRUE, # filter on active datasets
            ][run_tasks, on="task", nomatch=0L # filter for env var RUN_TASKS
              ][, c("active","gb","rows","cardinality","id","seq","path") := NULL # remove unused, id+seq to be used for join
                ][]

solution = rbindlist(list(
  dask = list(task=c("groupby","join","sort")),
  data.table = list(task=c("groupby","join","sort")),
  dplyr = list(task=c("groupby","join","sort")),
  juliadf = list(task=c("groupby","join")),
  modin = list(task=c("sort")),
  pandas = list(task=c("groupby","join","sort")),
  pydatatable = list(task=c("groupby","join","sort")),
  spark = list(task=c("groupby","join","sort"))
), idcol="solution")
solution = solution[run_solutions, on="solution", nomatch=0L] # filter for env var RUN_SOLUTIONS

format = rbindlist(list( # to be updated when binary files in place and benchmark scripts updated
  dask = list(format="csv"), # dask/dask#1277
  data.table = list(format="fst"),
  dplyr = list(format="fst"),
  juliadf = list(format="csv"), # JuliaData/Feather.jl#97
  modin = list(format="csv"), # modin-project/modin#278
  pandas = list(format="fea"),
  pydatatable = list(format="csv"), # h2oai/datatable#1461
  spark = list(format="csv") # https://stackoverflow.com/questions/53569580/read-feather-file-into-spark
), idcol="solution")

# what to run
dt = solution[data, on="task", allow.cartesian=TRUE]
dt[, "nodename" := nodename]
dt = format[dt, on="solution"]

# filter runs to only what is new
if (file.exists("time.csv") && file.exists("logs.csv") && nrow(timings<-fread("time.csv")) && nrow(logs<-fread("logs.csv"))) {
  timings[, .N,, c("nodename","batch","solution","task","data","version","git")
          ][, "N" := NULL
            ][!nzchar(git), "git" := NA_character_
              ][] -> timings
  logs[, .N,, c("nodename","batch","solution","task","data","version","git")
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
      this_run = dt[.(s, t, d), on=c("solution","task","data")]
      if (nrow(this_run) != 1L)
        stop(sprintf("single run for %s-%s-%s has %s entries while it must have exactly one", s, t, d, nrow(this_run)))
      ns = gsub(".", "", s, fixed=TRUE)
      out_dir = "out"
      out_file = sprintf("%s/run_%s_%s_%s.out", out_dir, ns, t, d)
      err_file = sprintf("%s/run_%s_%s_%s.err", out_dir, ns, t, d)
      if (!is.na(this_run$run_batch)) {
        comment = sprintf(": %s run on %s", substr(this_run$compare, 1, 7), format(as.Date(as.POSIXct(this_run$run_batch, origin="1970-01-01")), "%Y%m%d"))
        log_run(s, t, d, action="skip", batch=batch, nodename=nodename, stderr=wcl(err_file), comment=comment) # skip also logs number of lines stderr from previos run
        next
      }
      log_run(s, t, d, action="start", batch=batch, nodename=nodename)
      # TODO SRC_GRP_LOCAL is groupby specific
      Sys.setenv("SRC_GRP_LOCAL"=this_run[, paste(data, format, sep=".")])
      if (file.exists(out_file)) file.remove(out_file)
      if (file.exists(err_file)) file.remove(err_file)
      ext = file.ext(s)
      cmd = sprintf("./%s/%s-%s.%s > %s 2> &s", ns, t, ns, ext, out_file, err_file)
      venv = if (ext=="py") sprintf("source ./%s/py-%s/bin/activate && ", ns, ns) else ""
      shcmd = sprintf("/bin/bash -c \"%s%s\"", venv, cmd)
      system(shcmd) # here script actually runs
      Sys.unsetenv("SRC_GRP_LOCAL")
      log_run(s, t, d, action="finish", batch=batch, nodename=nodename, stderr=wcl(err_file))
    }
  }
}
