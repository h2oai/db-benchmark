library("data.table")
if (packageVersion("data.table") <= "1.12.0") stop("db-benchmark launcher script depends on recent data.table features, install at least 1.12.0. If you need to benchmark older data.table tweak script to use custom library where older version is installed.")
source("./_helpers/lib-launcher.R")

is.sigint()

batch = Sys.getenv("BATCH", NA)
.nodename = Sys.info()[["nodename"]]
mockup = as.logical(Sys.getenv("MOCKUP", "false"))
forcerun = as.logical(Sys.getenv("FORCE_RUN", "false"))

run_tasks = getenv("RUN_TASKS") # run_tasks = c("groupby","join")
if (!length(run_tasks)) q("no")
run_solutions = getenv("RUN_SOLUTIONS") # run_solutions = c("data.table","dplyr","pydatatable","spark","pandas")
if (!length(run_solutions)) q("no")

data = fread("./_control/data.csv", logical01=TRUE)
data[active==TRUE, # filter on active datasets
     ][run_tasks, on="task", nomatch=NA # filter for env var RUN_TASKS
       ][, c("active") := NULL # remove unused
         ][] -> data
if (any(is.na(data$data))) stop("missing entries in ./_control/data.csv for some tasks")

timeout = fread("./_control/timeout.csv", colClasses=c("character","character","numeric"))
timeout[run_tasks, on="task", nomatch=NA #  # filter for env var RUN_TASKS
        ] -> timeout
if (any(is.na(timeout$minutes))) stop("missing entries in ./_control/timeout.csv for some tasks")

solution = fread("./_control/solutions.csv")
solution[run_solutions, on="solution", nomatch=NA # filter for env var RUN_SOLUTIONS
         ] -> solution
if (any(is.na(solution$task))) stop("missing entries in ./_control/solutions.csv for some solutions")

# what to run, log machine name, lookup timeout
dt = solution[data, on="task", allow.cartesian=TRUE]
dt[, "nodename" := .nodename]
dt[, "in_rows" := substr(data, 4L, 6L)]
dt[timeout, "timeout_s" := i.minutes*60, on=c("task","in_rows")]
if (any(is.na(dt$timeout_s))) stop("missing entries in ./_control/timeout.csv for some tasks, detected after joining to solutions and data to run")

# TODO better translation, remove G2 from data.csv
# "G2" grouping data only relevant for clickhouse, so filter out "G2" for other solutions
dt = dt[!(substr(data, 1L, 2L)=="G2" & solution!="clickhouse")]
# clickhouse memory table engine "G1", disabled as per #91
dt = dt[!(substr(data, 1L, 2L)=="G1" & solution=="clickhouse")]

# filter runs to only what is new (TODO put to helper function)
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

# run (TODO put to helper function)
out_dir = "out"

## solution
solutions = dt[, unique(solution)]
for (s in solutions) { #s = solutions[1]
  ns = gsub(".", "", s, fixed=TRUE) # no dots in paths
  ext = file.ext(s)
  if (!length(ext)) stop(sprintf("solution %s does not have file extension defined in file.ext helper function", s))
  venv = if (ext=="py") {
    # https://stackoverflow.com/questions/52779016/conda-command-working-in-command-prompt-but-not-in-bash-script
    if (ns%in%c("cudf")) sprintf("source ~/anaconda3/etc/profile.d/conda.sh && conda activate %s && ", ns)
    else sprintf("source ./%s/py-%s/bin/activate && ", ns, ns)
  } else ""
  ### task
  tasks = dt[.(s), unique(task), on="solution"]
  for (t in tasks) { #t = tasks[1]
    data_name_env = if (t=="groupby") "SRC_GRP_LOCAL" else if (t=="join") "SRC_JN_LOCAL" else stop("new task has to be added in launcher.R script too")
    #### data
    data = dt[.(s, t), data, on=c("solution","task")]
    for (d in data) { #d=data[1]
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
      eval(as.call( # workaround for dynamic LHS in: Sys.setenv(as.name(data_name_env)=d)
        c(list(quote(Sys.setenv)), setNames(list(d), data_name_env))
      ))
      if (!mockup) {
        if (file.exists(out_file)) file.remove(out_file)
        if (file.exists(err_file)) file.remove(err_file)
      }
      localcmd = if (ext=="sql") { # only clickhouse for now
        sprintf("exec.sh %s %s", t, d)
      } else sprintf("%s-%s.%s", t, ns, ext)
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
