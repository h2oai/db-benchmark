library(data.table)

batch = Sys.getenv("BATCH", NA)
nodename = Sys.info()[["nodename"]]

log_run = function(solution, task, data, finished, batch, nodename, verbose=TRUE) {
  timestamp=as.numeric(Sys.time())
  lg = data.table(nodename=nodename, batch=batch, solution=solution, task=task, data=data, timestamp=timestamp, finished=finished)
  file = "logs.csv"
  fwrite(lg, file=file, append=file.exists(file), col.names=!file.exists(file))
  if (verbose) cat(sprintf("%s %s %s %s\n", if (finished) "starting" else "finished", solution, task, data))
}
file.ext = function(x)
  switch(x,
         "data.table"=, "dplyr"="R",
         "pandas"=, "spark"=, "pydatatable"=, "modin"=, "dask"="py",
         "juliadf"="jl")
getenv = function(x) {
  v = Sys.getenv(x, NA_character_)
  if (is.na(v)) stop(sprintf("%s env var not defined.", x))
  v = strsplit(v, " ", fixed=TRUE)[[1L]]
  if (length(v)!=length(unique(v))) stop(sprintf("%s contains non-unique values", x))
  v
}
run_tasks = getenv("RUN_TASKS")
#run_tasks = "groupby"
run_solutions = getenv("RUN_SOLUTIONS")
#run_solutions=c("data.table","dplyr","pydatatable")

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
  dask = list(format="csv"),
  data.table = list(format="csv"),
  dplyr = list(format="csv"),
  juliadf = list(format="csv"),
  modin = list(format="csv"),
  pandas = list(format="csv"),
  pydatatable = list(format="csv"),
  spark = list(format="csv")
), idcol="solution")

# what to run
dt = solution[data, on="task", allow.cartesian=TRUE]
dt = format[dt, on="solution"]

# filter runs to only what is new
#TODO

# run

## solution
solutions = dt[, unique(solution)]
for (s in solutions) {
  ### task
  #s = solutions[1]
  tasks = dt[.(s), unique(task), on="solution"]
  for (t in tasks) {
    #### data
    #t = tasks[1]
    data = dt[.(s, t), data, on=c("solution","task")]
    for (d in data) {
      #d=data[1]
      this_run = dt[.(s, t, d), on=c("solution","task","data")]
      if (nrow(this_run) != 1L) stop(sprintf("single run for %s-%s-%s has %s entries while it must have exactly one", s, t, d, nrow(this_run)))
      log_run(s, t, d, finished=0, batch=batch, nodename=nodename)
      # TODO SRC_GRP_LOCAL is groupby specific
      Sys.setenv("SRC_GRP_LOCAL"=this_run[, paste(data, format, sep=".")])
      ns = gsub(".", "", s, fixed=TRUE)
      out_dir = "out"
      out_file = sprintf("%s/run_%s_%s_%s_%s.out", out_dir, batch, ns, t, d)
      ext = file.ext(s)
      cmd = sprintf("./%s/%s-%s.%s > %s 2>&1", ns, t, ns, ext, out_file)
      venv = if (ext=="py") sprintf("source ./%s/py-%s/bin/activate && ", ns, ns) else ""
      shcmd = sprintf("/bin/bash -c \"%s%s\"", venv, cmd)
      system(shcmd) # here script actually runs
      Sys.unsetenv("SRC_GRP_LOCAL")
      log_run(s, t, d, finished=1, batch=batch, nodename=nodename)
    }
  }
}
