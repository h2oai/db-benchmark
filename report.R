library(data.table)
stopifnot(requireNamespace("knitr", quietly=TRUE))
kk = knitr::kable
get_report_status_file = function(path=getwd()) {
  file.path(path, "report-done")
}
get_report_solutions = function() {
  c("data.table", "dplyr", "pandas", "pydatatable", "spark", "dask", "juliadf")
}

# load ----

load_time = function(path=getwd()) {
  fread(file.path(path, "time.csv"))[
    !is.na(batch) &
      in_rows %in% c(1e7, 1e8, 1e9) &
      solution %in% get_report_solutions()
    ][order(timestamp)]
}
load_logs = function(path=getwd()) {
  fread(file.path(path, "logs.csv"))[
    !is.na(batch) &
      nzchar(solution) &
      solution %in% get_report_solutions() &
      action %in% c("start","finish")
    ][order(timestamp)]
}
load_questions = function(path=getwd()) {
  fread(file.path(path, "questions.csv"))
}

# clean ----

clean_time = function(d) {
  if (nrow(d[!nzchar(version) | is.na(version)]))
    stop("timings data contains NA or '' as version field, that should not happen")
  d[!nzchar(git), git := NA_character_
    ][task=="groupby" & solution=="spark" & batch<1546755894, out_cols := NA_integer_ # spark initially was not returning grouping columns, this has been fixed starting from batch 1546755894
      ][task=="groupby" & solution%in%c("pandas","dask"), "out_cols" := NA_integer_ # pandas and dask could return correct out_cols: https://github.com/h2oai/db-benchmark/issues/68
        ][task=="groupby" & solution=="dask" & question%in%c("max v1 - min v2 by id2 id4","regression v1 v2 by id2 id4"), "out_rows" := NA_integer_ # verify correctness of syntax and answer: https://github.com/dask/dask/issues/4372
          ][, `:=`(nodename=ft(nodename), in_rows=ft(in_rows), question=ft(question), solution=ft(solution), fun=ft(fun), version=ft(version), git=ft(git), task=ft(task), data=ft(data))
            ][]
}
clean_logs = function(l) {
  if (nrow(l[!nzchar(version) | is.na(version)]))
    stop("logs data contains NA or '' as version field, that should not happen")
  l[!nzchar(git), git := NA_character_
    ][, `:=`(nodename=ft(nodename), solution=ft(solution), version=ft(version), git=ft(git), task=ft(task), data=ft(data), action=ft(action))
      ][]
}
clean_questions = function(q) {
  q[, `:=`(task=ft(task), question=ft(question), question_group=ft(question_group))
    ][]
}

# model ----

model_time = function(d) {
  if (nrow(d[!is.na(chk), .(unq_chk=uniqueN(chk)), .(task, solution, data, question)][unq_chk>1]))
    stop("Value of 'chk' varies for different runs for single solution+question")
  if (nrow(d[!is.na(out_rows), .(unq_out_rows=uniqueN(out_rows)), .(task, solution, data, question)][unq_out_rows>1]))
    stop("Value of 'out_rows' varies for different runs for single solution+question")
  if (nrow(d[!is.na(out_cols), .(unq_out_cols=uniqueN(out_cols)), .(task, solution, data, question)][unq_out_cols>1]))
    stop("Value of 'out_cols' varies for different runs for single solution+question")
  if (nrow(d[!is.na(cache), .(unq_cache=uniqueN(cache))][unq_cache>1]))
    stop("Value of 'cache' should be constant for all solutions")
  d = dcast(d, nodename+batch+in_rows+question+solution+fun+version+git+task+data ~ run, value.var=c("timestamp","time_sec","mem_gb","chk_time_sec","chk","out_rows","out_cols"))
  d[, c("chk_2","out_rows_2","out_cols_2") := NULL]
  setnames(d, c("chk_1","out_rows_1","out_cols_1"), c("chk","out_rows","out_cols"))
  d
}
model_logs = function(l) {
  l = dcast(l, nodename+batch+solution+version+git+task+data ~ action, value.var=c("timestamp","stderr"))
  l[, stderr_start := NULL]
  setnames(l, c("stderr_finish","timestamp_start","timestamp_finish"), c("script_stderr","script_start","script_finish"))
  l
}
model_questions = function(q) {
  q
}

# merge ----

.merge_time_logs = function(d, l) {
  warning("deprecated, use merge_logs_questions followed by merge_time_logsquestions")
  ld = d[l, on=c("nodename","batch","solution","task","data"), nomatch=NA]
  if (nrow(ld[as.character(version)!=as.character(i.version)]))
    stop("Solution version in 'version' does not match between 'time' and 'logs'")
  if (nrow(ld[as.character(git)!=as.character(i.git)]))
    stop("Solution revision in 'git' does not match between 'time' and 'logs'")
  ld[, c("i.version","i.git") := NULL]
  ld
}
merge_logs_questions = function(l, q) {
  grain_l = l[, c(list(ii=1L), .SD), c("nodename","batch","solution","task","data")]
  lq = copy(q)[, "ii":=1L # used for cartesian product
               ][grain_l, on=c("task","ii"), allow.cartesian=TRUE, j=.(
                 nodename, batch, solution, task, data,
                 question=x.question, question_group=x.question_group,
                 version=i.version, git=i.git,
                 script_start=i.script_start, script_finish=i.script_finish, script_stderr=i.script_stderr
               )]
  lq
}
merge_time_logsquestions = function(d, lq) {
  ld = d[lq, on=c("nodename","batch","solution","task","data","question"),
         nomatch=NULL] # filter out timings for which logs were invalid or uncompleted
  if (nrow(ld[as.character(version)!=as.character(i.version)])) # one side NAs are skipped
    stop("Solution version in 'version' does not match between 'time' and 'logs', different 'version' reported from solution script vs launcher script")
  if (nrow(ld[as.character(git)!=as.character(i.git)])) # one side NAs are skipped
    stop("Solution revision in 'git' does not match between 'time' and 'logs', , different 'git' reported from solution script vs launcher script")
  ld = d[lq, on=c("nodename","batch","version","git","solution","task","data","question"), nomatch=NA, allow.cartesian=TRUE] # re-join to get i's version git
  ld
}

# transform ----

ft = function(x) {
  factor(x, levels=unique(x))
}
ftdata = function(x, task="groupby") {
  labsorted = function(x) {
    ans = rep("unsorted", length(x))
    ans[as.logical(as.integer(x))] = "sorted"
    ans
  }
  if (task=="groupby") {
    y = strsplit(as.character(x), "_", fixed = TRUE)
    in_rows=ft(sapply(y, `[`, 2L))
    k=ft(sapply(y, `[`, 3L))
    na=ft(sapply(y, `[`, 4L))
    sorted=ft(labsorted(sapply(y, `[`, 5L)))
    nasorted=ft(sprintf("%s%% NAs, %s", as.character(na), as.character(sorted)))
    list(in_rows=in_rows, k=k, na=na, sorted=sorted, nasorted=nasorted)
  } else {
      stop("no other task defined for decompose_dataname")
  }
}
transform = function(ld) {
  ld[, max_batch:=max(batch), c("nodename","solution","task","data")]
  ld[, script_recent:=FALSE][batch==max_batch, script_recent:=TRUE][, max_batch:=NULL]
  ld[, "na_time_sec":=FALSE][is.na(time_sec_1) | is.na(time_sec_2), "na_time_sec":=TRUE]
  ld[, c(list(nodename=nodename, batch=batch, ibatch=as.integer(ft(as.character(batch))), solution=solution,
              question=question, question_group=question_group, fun=fun, version=version, git=git, task=task, data=data),
         ftdata(data), .SD),
     .SDcols=c(paste(rep(c("timestamp","time_sec","mem_gb","chk_time_sec"), each=2), 1:2, sep="_"),
               paste("script", c("finish","start","stderr","recent"), sep="_"),
               "na_time_sec","out_rows","out_cols")
     ][, `:=`(iquestion=as.integer(question), script_time_sec=script_finish-script_start)
       ][] -> lld
  lld
}

# all ----

time_logs = function(path=getwd()) {
  d = model_time(clean_time(load_time(path=path)))
  l = model_logs(clean_logs(load_logs(path=path)))
  q = model_questions(clean_questions(load_questions(path=path)))
  
  lq = merge_logs_questions(l, q)
  ld = merge_time_logsquestions(d, lq)
  
  lld = transform(ld)
  lld
}
