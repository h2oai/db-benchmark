library(data.table)
kk = knitr::kable
get_report_status_file = function() {
  "report-done"
}

# load ----

load_time = function() {
  fread("~/git/db-benchmark/time.csv")[
    !is.na(batch) &
      in_rows %in% c(1e7, 1e8, 1e9) &
      solution %in% c("data.table", "dplyr", "pandas", "pydatatable", "spark", "dask", "juliadf") &
      question %in% c("sum v1 by id1", "sum v1 by id1:id2", "sum v1 mean v3 by id3", "mean v1:v3 by id4", "sum v1:v3 by id6")
    ][order(timestamp)]
}
load_logs = function() {
  fread("~/git/db-benchmark/logs.csv")[
    !is.na(batch) &
      nzchar(solution) &
      solution %in% c("data.table", "dplyr", "pandas", "pydatatable", "spark", "dask", "juliadf") &
      action %in% c("start","finish")
    ][order(timestamp)]
}

# clean ----

clean_time = function(d) {
  d[!nzchar(git), git := NA_character_]
}
clean_logs = function(l) {
  l[!nzchar(git), git := NA_character_]
}

# model ----

model_time = function(d) {
  if (nrow(d[, .(unq_chk=uniqueN(chk)), .(task, solution, data, question)][unq_chk>1]))
    stop("Value of 'chk' varies for different runs for single solution+question")
  if (nrow(d[, .(unq_out_rows=uniqueN(out_rows)), .(task, solution, data, question)][unq_out_rows>1]))
    stop("Value of 'out_rows' varies for different runs for single solution+question")
  if (nrow(d[, .(unq_out_cols=uniqueN(out_cols)), .(task, solution, data, question)][unq_out_cols>1]))
    stop("Value of 'out_cols' varies for different runs for single solution+question")
  if (nrow(d[, .(unq_cache=uniqueN(cache))][unq_cache>1]))
    stop("Value of 'cache' should be constant for all solutions")
  d = dcast(d, ft(nodename)+batch+ft(in_rows)+ft(question)+ft(solution)+ft(fun)+ft(version)+ft(git)+ft(task)+ft(data) ~ run, value.var=c("timestamp","time_sec","mem_gb","chk_time_sec","chk","out_rows","out_cols"))
  d[, c("chk_2","out_rows_2","out_cols_2") := NULL]
  setnames(d, c("chk_1","out_rows_1","out_cols_1"), c("chk","out_rows","out_cols"))
  d
}
model_logs = function(l) {
  l = dcast(l, ft(nodename)+batch+ft(solution)+ft(version)+ft(git)+ft(task)+ft(data) ~ ft(action), value.var=c("timestamp","stderr"))
  l[, stderr_start := NULL]
  setnames(l, c("stderr_finish","timestamp_start","timestamp_finish"), c("script_stderr","script_start","script_finish"))
}

# merge ----

merge_time_logs = function(d, l) {
  ld = d[l, on=c("nodename","batch","solution","task","data"), nomatch=NA]
  if (nrow(ld[as.character(version)!=as.character(i.version)]))
    stop("Solution version in 'version' does not match between 'time' and 'logs'")
  if (nrow(ld[as.character(git)!=as.character(i.git)]))
    stop("Solution revision in 'git' does not match between 'time' and 'logs'")
  ld[, c("i.version","i.git") := NULL]
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
  ld[, c(list(nodename=nodename, batch=batch, ibatch=as.integer(ft(as.character(batch))), solution=solution,
              question=question, fun=fun, version=version, git=git, task=task, data=data),
         ftdata(data), .SD),
     .SDcols=c(paste(rep(c("timestamp","time_sec","mem_gb","chk_time_sec"), each=2), 1:2, sep="_"),
               paste("script", c("finish","start","stderr","recent"), sep="_"),
               "out_rows","out_cols")
     ][, `:=`(iquestion=as.integer(question), script_time_sec=script_finish-script_start)
       ][] -> lld
  lld
}

# all ----

time_logs = function() {
  d = load_time()
  d = clean_time(d)
  d = model_time(d)
  l = load_logs()
  l = clean_logs(l)
  l = model_logs(l)
  ld = merge_time_logs(d, l)
  transform(ld)
}
