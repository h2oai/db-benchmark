library(data.table)
stopifnot(requireNamespace("knitr", quietly=TRUE))
kk = knitr::kable
get_report_status_file = function(path=getwd()) {
  file.path(path, "report-done")
}
get_report_solutions = function() {
  c("data.table", "dplyr", "pandas", "pydatatable", "spark", "dask", "juliadf", "clickhouse", "cudf")
}
get_excluded_batch = function() {
  c(
    1552478772L, 1552482879L # testing different data as 1e9_1e2_0_0 to test logical compression of measures
    , 1552454531L, 1555929111L, 1555754148L # dl11 testing
    )
}

# load ----

load_time = function(path=getwd()) {
  time.csv = Sys.getenv("CSV_TIME_FILE","time.csv")
  fread(file.path(path,time.csv))[
    !is.na(batch) &
      in_rows %in% c(1e7, 1e8, 1e9) &
      solution %in% get_report_solutions() &
      !batch %in% get_excluded_batch()
    ][order(timestamp)]
}
load_logs = function(path=getwd()) {
  logs.csv = Sys.getenv("CSV_LOGS_FILE","logs.csv")
  fread(file.path(path,logs.csv))[
    !is.na(batch) &
      nzchar(solution) &
      solution %in% get_report_solutions() &
      action %in% c("start","finish") &
      !batch %in% get_excluded_batch()
    ][order(timestamp)]
}
load_questions = function(path=getwd()) {
  fread(file.path(path, "_control/questions.csv"))
}

# clean ----

clean_time = function(d) {
  if (nrow(d[!nzchar(version) | is.na(version)]))
    stop("timings data contains NA or '' as version field, that should not happen")
  old_advanced_groupby_questions = c("median v3 sd v3 by id2 id4","max v1 - min v2 by id2 id4","largest two v3 by id2 id4","regression v1 v2 by id2 id4","sum v3 count by id1:id6")
  d[!nzchar(git), git := NA_character_
    ][,"on_disk" := as.logical(on_disk)
      ][task=="groupby" & solution%in%c("pandas","dask","spark") & batch<1558106628, "out_cols" := NA_integer_
        ][task=="groupby" & solution=="dask" & batch<1558106628 & question%in%c("max v1 - min v2 by id2 id4","regression v1 v2 by id2 id4"), c("out_rows","out_cols","chk") := .(NA_integer_, NA_integer_, NA_character_)
          ][task=="groupby" & solution=="pandas" & batch<=1558106628 & question=="largest two v3 by id2 id4", "out_cols" := NA_integer_
            ][task=="groupby" & solution=="spark" & batch<1548084547, "chk_time_sec" := NA_real_ # spark chk calculation speed up, NA to make validation work on bigger threshold
              ][task=="groupby" & question%in%old_advanced_groupby_questions & batch<1573882448, c("out_rows","out_cols","chk") := list(NA_integer_, NA_integer_, NA_character_)
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
  # chk tolerance for cudf disabled as of now: https://github.com/rapidsai/cudf/issues/2494
  #d[!is.na(chk) & solution=="cudf", .(unq_chk=paste(unique(chk), collapse=","), unqn_chk=uniqueN(chk)), .(task, solution, data, question)][unqn_chk>1L]
  # as well for dask due to #136
  #d[!is.na(chk) & solution=="dask", .(unq_chk=paste(unique(chk), collapse=","), unqn_chk=uniqueN(chk)), .(task, solution, data, question)][unqn_chk>1L] 
  if (nrow(
    d[!is.na(chk) & (
      solution!="cudf" &
      solution!="dask"
      ),
      .(unqn_chk=uniqueN(chk)), .(task, solution, data, question)][unqn_chk>1L]
    ))
    stop("Value of 'chk' varies for different runs for single solution+question")
  if (nrow(d[!is.na(out_rows), .(unqn_out_rows=uniqueN(out_rows)), .(task, solution, data, question)][unqn_out_rows>1L]))
    stop("Value of 'out_rows' varies for different runs for single solution+question")
  if (nrow(d[!is.na(out_cols), .(unqn_out_cols=uniqueN(out_cols)), .(task, solution, data, question)][unqn_out_cols>1L]))
    stop("Value of 'out_cols' varies for different runs for single solution+question")
  if (nrow(d[!is.na(out_rows), .(unqn_out_rows=uniqueN(out_rows)), .(task, data, question)][unqn_out_rows>1L]))
    stop("Value of 'out_rows' varies for different runs for single question")
  #d[,.SD][!is.na(out_rows), `:=`(unq_out_rows=uniqueN(out_rows), paste_unq_out_rows=paste(unique(out_rows), collapse=",")), .(task, data, question)][unq_out_rows>1, .(paste_unq_out_rows), .(task, solution, data, question)]
  if (nrow(d[!is.na(out_cols), .(unqn_out_cols=uniqueN(out_cols)), .(task, data, question)][unqn_out_cols>1L]))
    stop("Value of 'out_cols' varies for different runs for single question")
  if (nrow(d[, .(unqn_on_disk=uniqueN(on_disk)), .(task, solution, data, batch)][unqn_on_disk>1L]))
    stop("Value of 'on_disk' varies for different questions/runs for single solution+data+batch") # on_disk should be const in script
  d = dcast(d, nodename+batch+in_rows+question+solution+fun+on_disk+cache+version+git+task+data ~ run, value.var=c("timestamp","time_sec","mem_gb","chk_time_sec","chk","out_rows","out_cols"))
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
  ld = d[lq, on=c("nodename","batch","version","git","solution","task","data","question"), nomatch=NA] # re-join to get i's version git
  ld
}

# transform ----

ft = function(x) {
  factor(x, levels=unique(x))
}
ftdata = function(x, task) {
  labsorted = function(x) {
    ans = rep("unsorted data", length(x))
    ans[as.logical(as.integer(x))] = "pre-sorted data"
    ans
  }
  if (all(task %in% c("groupby","join"))) {
    y = strsplit(as.character(x), "_", fixed = TRUE)
    y = lapply(y, function(yy) {yy[yy=="NA"] = NA_character_; yy})
    in_rows=ft(sapply(y, `[`, 2L))
    k=ft(sapply(y, `[`, 3L))
    na=ft(sapply(y, `[`, 4L))
    sort=ft(sapply(y, `[`, 5L))
    sorted=ft(labsorted(levels(sort)))
    nasorted=ft(sprintf("%s%% NAs, %s", as.character(na), as.character(sorted)))
    labk = character(length(k))
    labk[!is.na(k)] = sprintf("%s cardinality factor, ", as.character(k[!is.na(k)]))
    knasorted=ft(sprintf("%s%s", labk, as.character(nasorted)))
    list(in_rows=in_rows, k=k, na=na, sort=sort, sorted=sorted, nasorted=nasorted, knasorted=knasorted)
  } else {
    stop("no task defined for ftdata other than groupby and join")
  }
}
transform = function(ld) {
  ld[, max_batch:=max(batch), c("solution","task","data")]
  ld[, script_recent:=FALSE][batch==max_batch, script_recent:=TRUE][, max_batch:=NULL]
  ld[, "na_time_sec":=FALSE][is.na(time_sec_1) | is.na(time_sec_2), "na_time_sec":=TRUE]
  ld[, "on_disk" := on_disk[1L], by=c("batch","solution","task","data")] # on_disk is a constant across whole script, fill trailing NA so advanced question group will not stay NA if basic had that info #126

  { # clickhouse memory/mergetree table engine handling
    ld[, "engine":=NA_character_]
    ld[task=="groupby" & solution=="clickhouse" & substr(data, 1L, 2L)=="G1", engine:="memory"]
    ld[task=="groupby" & solution=="clickhouse" & substr(data, 1L, 2L)=="G2", engine:="mergetree"]
    ## according to #91 we now will present mergetree only
    ld = ld[!(task=="groupby" & solution=="clickhouse" & engine=="memory")]
    ld[task=="groupby" & solution=="clickhouse" & engine=="mergetree", `:=`(
      data = gsub("G2", "G1", data, fixed=TRUE),
      on_disk = !on_disk ## swap to denote slower method with star suffix, so for clickhouse it is (currently unused) memory table engine, otherwise clickhouse would always be marked by star #126
    )]
    #if (nrow(ld[task=="groupby" & solution=="clickhouse" & engine=="memory" & na_time_sec==TRUE])) {
    #  ld[task=="groupby" & solution=="clickhouse" & engine=="mergetree"
    #     ][, `:=`(
    #       disk_na_time_sec=na_time_sec, # original na_time_sec
    #       disk_time_sec_1=time_sec_1, disk_time_sec_2=time_sec_2,
    #       disk_timestamp_1=timestamp_1, disk_timestamp_2=timestamp_2,
    #       disk_engine=engine,
    #       disk_fun=fun,
    #       disk_script_stderr=script_stderr,
    #       data=gsub("G2", "G1", data, fixed=TRUE),  # only to join to G1 timings
    #       na_time_sec=TRUE                          # only to join to na_time_sec=TRUE
    #     )] -> ch_disk_time
    #  ld[ch_disk_time, on=c("batch","task","solution","data","question","na_time_sec"),
    #     `:=`(time_sec_1=i.disk_time_sec_1, time_sec_2=i.disk_time_sec_2,
    #          timestamp_1=i.disk_timestamp_1, timestamp_2=i.disk_timestamp_2,
    #          fun=i.disk_fun, na_time_sec=i.disk_na_time_sec, engine=i.disk_engine, script_stderr=i.disk_script_stderr)]
    #}
  }
  
  ld[, c(list(nodename=nodename, batch=batch, ibatch=as.integer(ft(as.character(batch))), solution=solution,
              question=question, question_group=question_group, fun=fun, on_disk=on_disk, cache=cache, version=version, git=git, task=task, data=data, engine=engine),
         ftdata(data, task=as.character(task)), .SD),
     .SDcols=c(paste(rep(c("timestamp","time_sec","mem_gb","chk_time_sec"), each=2), 1:2, sep="_"),
               paste("script", c("finish","start","stderr","recent"), sep="_"),
               "na_time_sec","out_rows","out_cols","chk")
     ][, `:=`(script_time_sec=script_finish-script_start)
       ][] -> lld
  lld[, "iquestion":=as.integer(droplevels(question)), by="task"]
  lld[]
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
