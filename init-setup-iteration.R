#!/usr/bin/env Rscript

cat("# init-setup-iteration.R\n")

run_tasks = Sys.getenv("RUN_TASKS", NA_character_)
if (is.na(run_tasks)) stop("RUN_TASKS env var not defined.")
run_tasks = strsplit(run_tasks, " ", fixed=TRUE)[[1L]]

suppressPackageStartupMessages(library(data.table))
iters_on_task_level = function(){
  dt = fread("data.csv")
  dt = dt[active==TRUE # flag
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
     .(iter=sprintf("export %s", paste(paste(c("SRC_GRP","SRC_GRP_LOCAL"), c(hdfs, local), sep="="), collapse=" "))),
     .(task, data)
     ][, if(.N) writeLines(iter, con=file.path("loop-groupby-data.env"), sep="\n"), .(task)]
  dt[task=="sort",
     .(iter=sprintf("export %s", paste(paste(c("SRC_X","SRC_X_LOCAL"), c(hdfs[1L], local[1L]), sep="="), collapse=" "))),
     .(task, rows)
     ][, if(.N) writeLines(iter, con=file.path("loop-sort-data.env"), sep="\n"), .(task)]
  dt[task=="read",
     .(iter=sprintf("export %s", paste(paste(c("SRC_GRP","SRC_GRP_LOCAL"), c(hdfs[1L], local[1L]), sep="="), collapse=" "))),
     .(task, rows)
     ][, if(.N) writeLines(iter, con=file.path("loop-read-data.env"), sep="\n"), .(task)]
  invisible()
}
iters_on_task_level()

if ( !interactive() ) quit("no", status=0)
