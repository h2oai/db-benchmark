library(data.table)
suppressPackageStartupMessages(library(dplyr))
stopifnot(requireNamespace("knitr", quietly=TRUE))
kk = knitr::kable
get_report_status_file = function(path=getwd()) {
  file.path(path, "report-done")
}
get_report_solutions = function() {
  c("data.table", "dplyr", "pandas", "pydatatable", "spark", "dask", "juliadf", "juliads", "clickhouse", "cudf", "polars","arrow","duckdb", "duckdb-latest")
}
get_data_levels = function() {
  ## groupby
  in_rows = c("1e7","1e8","1e9")
  k_na_sort = c("1e2_0_0","1e1_0_0","2e0_0_0","1e2_0_1","1e2_5_0")
  groupby = paste("G1", paste(rep(in_rows, each=length(k_na_sort)), k_na_sort, sep="_"), sep="_")
  ## join
  in_rows = c("1e7","1e8","1e9")
  k_na_sort = c("NA_0_0","NA_5_0","NA_0_1")
  join = paste("J1", paste(rep(in_rows, each=length(k_na_sort)), k_na_sort, sep="_"), sep="_")
  ## groupby2014
  in_rows = c("1e7","1e8","1e9")
  k_na_sort = "1e2_0_0"
  groupby2014 = paste("G0", paste(rep(in_rows, each=length(k_na_sort)), k_na_sort, sep="_"), sep="_")
  list(groupby=groupby, join=join, groupby2014=groupby2014)
}
get_excluded_batch = function() {
  c(
    1552478772L, 1552482879L # testing different data as 1e9_1e2_0_0 to test logical compression of measures
    , 1552454531L, 1555929111L, 1555754148L # dl11 testing
    , 1619552039L, 1619596289L ## polars migration
    , 1609583373L ## clickhouse log timing issue
    , 1620737545L ## pydatatable unfinished run
    , 1592482882L ## clickhoue incompletely loaded table
    )
}

# load ----

load_time = function(path=getwd()) {
  time.csv = Sys.getenv("CSV_TIME_FILE","time.csv")
  fread(file.path(path,time.csv))[
    !is.na(batch) &
      in_rows %in% c(1e7, 1e8, 1e9) &
      solution %in% get_report_solutions() &
      !batch %in% get_excluded_batch() &
      !(task=="groupby" & substr(data, 1L, 2L)=="G2") &
      batch >= 1605961721
    ][order(timestamp)]
}
load_logs = function(path=getwd()) {
  logs.csv = Sys.getenv("CSV_LOGS_FILE","logs.csv")
  fread(file.path(path,logs.csv))[
    !is.na(batch) &
      nzchar(solution) &
      solution %in% get_report_solutions() &
      action %in% c("start","finish") &
      !batch %in% get_excluded_batch() &
      !(task=="groupby" & substr(data, 1L, 2L)=="G2") &
      batch >= 1605961721
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
                ][task=="groupby" & solution=="dask" & batch>=1609583373 & batch<Inf & question=="regression v1 v2 by id2 id4", c("out_rows","chk") := .(NA_integer_, NA_character_) ## change Inf to batch after upgrading to dask#7024
                ][solution=="polars" & batch<=1622492790, c("chk","out_rows") := list(NA_character_, NA_integer_) # polars NA handling broken in 0.7.19? #223
                ][, `:=`(nodename=ft(nodename), in_rows=ft(in_rows), question=ft(question), solution=ft(solution), fun=ft(fun), version=ft(version), git=ft(git), task=ft(task),
                         data=fctr(data, levels=unlist(get_data_levels())))
                    ][]
}
clean_logs = function(l) {
  if (nrow(l[!nzchar(version) | is.na(version)]))
    stop("logs data contains NA or '' as version field, that should not happen")
  l[!nzchar(git), git := NA_character_
    ][, `:=`(nodename=ft(nodename), solution=ft(solution), version=ft(version), git=ft(git), task=ft(task), data=fctr(data, levels=unlist(get_data_levels())), action=ft(action))
      ][]
}
clean_questions = function(q) {
  q[, `:=`(task=ft(task), question=ft(question), question_group=ft(question_group))
    ][]
}

# model ----

model_time = function(d) {
  if (!nrow(d))
    stop("timings is a 0 row table")
  #d[!is.na(chk) & solution=="dask", .(unq_chk=paste(unique(chk), collapse=","), unqn_chk=uniqueN(chk)), .(task, solution, data, question)][unqn_chk>1L] 
  approxUniqueN1 = function(x, tolerance=1e-3, debug=FALSE) { ## dask is fine on 1e-6,
    # message(paste('ok made it this far with x=',x))
    l = lapply(as.list(rbindlist(lapply(strsplit(as.character(x), ";", fixed=TRUE), as.list))), type.convert, as.is = TRUE)
    int = sapply(l, is.integer)
    dbl = sapply(l, is.double)
    if (sum(int, dbl)!=length(l)) stop("chk has elements that were not converted to int or double")
    ans = vector("logical", length(l))
    ans[int] = vapply(l[int], function(y) {same=uniqueN(y)==1L; if (debug&&!same) browser() else same}, NA)
    ans[dbl] = vapply(l[dbl], function(y, t) {
      m = mean(y)
      lb = m-abs(m)*t
      ub = m+abs(m)*t
      same = all(y >= lb) && all(y <= ub)
      if (debug&&!same) browser() else same
    }, t=tolerance, NA)
    all(ans)
  }
  # d[solution=="polars" & data%like%"G1_1e[7|8]_1e2_5_0" & run==1L & {z=tail(unique(batch, na.rm=TRUE), 3); print(z); batch%in%z}][, dcast(.SD, data+question~batch+version, value.var="chk")]
  if (nrow(
    d[!is.na(chk), .(unqn1_chk=approxUniqueN1(chk)), .(task, solution, data, question)][unqn1_chk==FALSE]
    )) stop("Value of 'chk' varies for different runs for single solution+question")
  #d[,.SD][!is.na(chk), `:=`(unq_chk=approxUniqueN1(chk), paste_unq_chk=paste(unique(chk), collapse=",")), .(task, data, question)][unq_chk==FALSE, .(paste_unq_chk), .(task, solution, data, question)]
  #d[solution=="polars" & data%like%"G1_1e[7|8]_1e2_5_0" & run==1L & {z=tail(unique(batch, na.rm=TRUE), 3); print(z); batch%in%z}][, dcast(.SD, data+question~batch+version, value.var="out_rows")]
  if (nrow(d[!is.na(out_rows), .(unqn_out_rows=uniqueN(out_rows)), .(task, solution, data, question)][unqn_out_rows>1L]))
    stop("Value of 'out_rows' varies for different runs for single solution+question")
  if (nrow(d[!is.na(out_cols), .(unqn_out_cols=uniqueN(out_cols)), .(task, solution, data, question)][unqn_out_cols>1L]))
    stop("Value of 'out_cols' varies for different runs for single solution+question")
  #d[,.SD][!is.na(out_cols), `:=`(unq_out_cols=uniqueN(out_cols), paste_unq_out_cols=paste(unique(out_cols), collapse=",")), .(task, data, question)][unq_out_cols>1, .(paste_unq_out_cols), .(task, solution, data, question)]
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

fctr = function(x, levels=unique(x), ..., rev=FALSE) {
  factor(x, levels=if (rev) rev(levels) else levels, ...)
}
ft = function(x) fctr(x)
ftdata = function(x, task) {
  labsorted = function(x) {
    ans = rep("unsorted data", length(x))
    ans[as.logical(as.integer(x))] = "pre-sorted data"
    ans
  }
  if (all(task %in% c("groupby","join","groupby2014"))) {
    y = strsplit(as.character(x), "_", fixed = TRUE)
    y = lapply(y, function(yy) {yy[yy=="NA"] = NA_character_; yy})
    in_rows=ft(sapply(y, `[`, 2L))
    k=ft(sapply(y, `[`, 3L))
    na=ft(sapply(y, `[`, 4L))
    sort=ft(sapply(y, `[`, 5L))
    sorted=ft(labsorted(as.character(sort)))
    nasorted=ft(sprintf("%s%% NAs, %s", as.character(na), as.character(sorted)))
    labk = character(length(k))
    labk[!is.na(k)] = sprintf("%s cardinality factor, ", as.character(k[!is.na(k)]))
    knasorted=ft(sprintf("%s%s", labk, as.character(nasorted)))
    list(in_rows=in_rows, k=k, na=na, sort=sort, sorted=sorted, nasorted=nasorted, knasorted=knasorted)
  } else {
    stop("task not defined for ftdata")
  }
}
transform = function(ld) {
  ld[, max_batch:=max(batch), c("solution","task","data")]
  ld[, script_recent:=FALSE][batch==max_batch, script_recent:=TRUE][, max_batch:=NULL]
  ld[, "na_time_sec":=FALSE][is.na(time_sec_1) | is.na(time_sec_2), "na_time_sec":=TRUE]
  ld[, "on_disk" := on_disk[1L], by=c("batch","solution","task","data")] # on_disk is a constant across whole script, fill trailing NA so advanced question group will not stay NA if basic had that info #126

  { # clickhouse memory/mergetree table engine handling for historical timings only, all new uses mergetree and G1 prefix #137
    ld[, "engine":=NA_character_]
    ld[task=="groupby" & solution=="clickhouse" & substr(data, 1L, 2L)=="G1" & batch<=1603737536, engine:="memory"]
    ld[task=="groupby" & solution=="clickhouse" & substr(data, 1L, 2L)=="G2" & batch<=1603737536, engine:="mergetree"]
    ld[task=="join" & solution=="clickhouse", engine:="mergetree"]
    if (nrow(ld[task=="groupby" & solution=="clickhouse" & substr(data, 1L, 2L)=="G2" & batch>1603737536]))
      stop("There should be no G2 prefix for clickhouse as we stopped using workaround for mandatory primary key and now using G1 for mergetree which was faster")
    ld[task=="groupby" & solution=="clickhouse" & substr(data, 1L, 2L)=="G1" & batch>1603737536, engine:="mergetree"]
    ## according to #91 we now will present mergetree only
    ld = ld[!(solution=="clickhouse" & engine=="memory")]
    ld[solution=="clickhouse" & engine=="mergetree", `:=`(
      on_disk = !on_disk ## swap to denote slower method with star suffix, so for clickhouse it is (currently unused) memory table engine, otherwise clickhouse would always be marked by star #126
    )]
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
  # complete set of data flag
  lld[, `:=`(task_dataN=uniqueN(data)),.(task)]
  lld[, "complete" := uniqueN(data)==task_dataN, .(task, solution, batch)]
  lld[, "task_dataN" := NULL]
  lld[]
}

# all ----

time_logs = function(path=getwd()) {
  ct = clean_time(load_time(path=getwd()))
  d = model_time(ct)
  l = model_logs(clean_logs(load_logs(path=path)))
  q = model_questions(clean_questions(load_questions(path=path)))
  
  lq = merge_logs_questions(l, q)
  ld = merge_time_logsquestions(d, lq)
  
  lld = transform(ld)
  lld
}

