#!/usr/bin/env Rscript

cat("# init-setup-iteration.R\n")

run_tasks = Sys.getenv("RUN_TASKS", NA_character_)
if (is.na(run_tasks)) stop("RUN_TASKS env var not defined.")
run_tasks = strsplit(run_tasks, " ", fixed=TRUE)[[1L]]
library(data.table)
iters_on_solution_level = function(){
  solution_dir = c("spark","impala","datatable","h2o","pandas","dask")
  dt = fread("data.csv")
  dt = dt[active==TRUE # flag
          ][run_tasks, on="task", nomatch=0L # filter for ENV VAR RUN TASKS
            ][data.table(active=TRUE, solution_dir=solution_dir), on="active", allow.cartesian=TRUE # cross join solutions
              ]
  dt[task=="join",
     .(iter=sprintf("export %s", paste(paste(c("SRC_X","SRC_Y","SRC_X_LOCAL","SRC_Y_LOCAL"), c(hdfs[1L], hdfs[2L], local[1L], local[2L]), sep="="), collapse=" "))),
     .(task, rows, solution_dir)
     ][, if(.N) writeLines(iter, con=file.path(solution_dir,"loop-join-data.env"), sep="\n"), .(task, solution_dir)]
  dt[task=="groupby",
     .(iter=sprintf("export %s", paste(paste(c("SRC_GRP","SRC_GRP_LOCAL"), c(hdfs, local), sep="="), collapse=" "))),
     .(task, solution_dir, data)
     ][, if(.N) writeLines(iter, con=file.path(solution_dir,"loop-groupby-data.env"), sep="\n"), .(task, solution_dir)]
  invisible()
}
iters_on_task_level = function(){
  dt = fread("data.csv")
  dt = dt[active==TRUE # flag
          ][run_tasks, on="task", nomatch=0L # filter for ENV VAR RUN TASKS
            ]
  dt[task=="join",
     .(iter=sprintf("export %s", paste(paste(c("SRC_X","SRC_Y","SRC_X_LOCAL","SRC_Y_LOCAL"), c(hdfs[1L], hdfs[2L], local[1L], local[2L]), sep="="), collapse=" "))),
     .(task, rows)
     ][, if(.N) writeLines(iter, con=file.path("loop-join-data.env"), sep="\n"), .(task)]
  dt[task=="groupby",
     .(iter=sprintf("export %s", paste(paste(c("SRC_GRP","SRC_GRP_LOCAL"), c(hdfs, local), sep="="), collapse=" "))),
     .(task, data)
     ][, if(.N) writeLines(iter, con=file.path("loop-groupby-data.env"), sep="\n"), .(task)]
  invisible()
}
iters_on_task_level()

if ( !interactive() ) quit("no", status=0)
