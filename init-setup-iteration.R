#!/usr/bin/env Rscript

cat("# init-setup-iteration.R\n")

suppressPackageStartupMessages(library(data.table))

run_tasks = Sys.getenv("RUN_TASKS", NA_character_)
if (is.na(run_tasks)) stop("RUN_TASKS env var not defined.")
run_tasks = strsplit(run_tasks, " ", fixed=TRUE)[[1L]]

write_data_envs = function(run_tasks) { # data envs by task
  if (length(run_tasks)!=length(unique(run_tasks))) stop("RUN_TASKS contains non-unique values")
  dt = fread("data.csv")
  dt = dt[active==TRUE,
          ][run_tasks, on="task", nomatch=0L # filter for ENV VAR RUN TASKS
            ]
  sapply(run_tasks, function(task) {
    fn = sprintf("loop-%s-data.env", task)
    if (file.exists(fn)) file.remove(fn)
  })
  dt[task=="join",
     .(iter=sprintf("export %s", paste(paste(c("SRC_X","SRC_Y","SRC_X_LOCAL","SRC_Y_LOCAL"), c(hdfs[1L], hdfs[2L], local[1L], local[2L]), sep="="), collapse=" "))),
     .(task, rows)
     ][, if(.N) writeLines(iter, con=file.path("loop-join-data.env"), sep="\n"), .(task)]
  dt[task=="groupby",
     .(iter=sprintf("export %s", paste("SRC_GRP_LOCAL", local, sep="="))),
     .(task, data)
     ][, if(.N) writeLines(iter, con=file.path("loop-groupby-data.env"), sep="\n"), .(task)]
  dt[task=="sort",
     .(iter=sprintf("export %s", paste(paste(c("SRC_X","SRC_X_LOCAL"), c(hdfs[1L], local[1L]), sep="="), collapse=" "))),
     .(task, rows)
     ][, if(.N) writeLines(iter, con=file.path("loop-sort-data.env"), sep="\n"), .(task)]
  dt[task=="read",
     .(iter=sprintf("export %s", paste("SRC_GRP_LOCAL", local[1L], sep="="))),
     .(task, rows)
     ][, if(.N) writeLines(iter, con=file.path("loop-read-data.env"), sep="\n"), .(task)]
  invisible()
}
write_data_envs(run_tasks)

run_solutions = Sys.getenv("RUN_SOLUTIONS", NA_character_)
if (is.na(run_solutions)) stop("RUN_SOLUTIONS env var not defined.")
run_solutions = strsplit(run_solutions, " ", fixed=TRUE)[[1L]]

write_solutions_envs = function(run_solutions) { # by solution and task!
  if (length(run_solutions)!=length(unique(run_solutions))) stop("RUN_SOLUTIONS contains non-unique values")
  fn = "do-solutions.env"
  if (file.exists(fn)) file.remove(fn)
  if (!length(run_solutions)) {
    file.create(fn)
    return(invisible())
  }
  envs_do_solutions = function(solutions, do) {
    stopifnot(length(solutions)==length(do))
    for (i in seq_along(solutions)) {
      writeLines(sprintf("export DO_%s=%s", toupper(gsub(".", "", solutions[i], fixed=TRUE)), tolower(do[i])), fn)
    }
  }
  do = rep(TRUE, length(run_solutions))
  envs_do_solutions(run_solutions, do)
  return(invisible())
  
  # TODO track version change and produce 'do' variable
  mynodename = Sys.info()[["nodename"]]
  lg = fread("logs.csv")[solution%in%run_solutions & nodename==mynodename]
  dt = fread("time.csv")[solution%in%run_solutions]
  current_version = function(solution) {
    solution
    run_tasks
  }
  last_run_version = function(solution) {
    solution
    run_tasks
  }
  if (!nrow(lg) || !nrow(dt)) { # might be new solution defined in run.conf which was not run before, or new machine
    
  }
  recent = dt[, .(last_batch=max(batch)), .(task, solution)]
  dt = dt[recent, on=c("task","solution","batch"="last_batch"), nomatch=NULL]
  dt = dt[order(solution)]
}
write_solutions_envs(run_solutions)

if ( !interactive() ) quit("no", status=0)
