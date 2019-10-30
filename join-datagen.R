# Rscript join-datagen.R 1e7 0 0 0 ## 1e7 rows, 0 ignored, 0% NAs, random order
# Rscript join-datagen.R 1e8 0 5 1 ## 1e8 rows, 0 ignored, 5% NAs, sorted order

# see h2oai/db-benchmark#106 for a design notes of this procedure, feedback welcome in the issue

# init ----

init = proc.time()[["elapsed"]]
args = commandArgs(TRUE)
N=as.numeric(args[1L]); K=as.integer(args[2L]); nas=as.integer(args[3L]); sort=as.integer(args[4L])
#N=1e6; K=NA_integer_; nas=0L; sort=0L
stopifnot(nas<=100L, nas>=0L, sort%in%c(0L,1L))
if (nas>0L) stop("'NA' not yet implemented")
if (sort==1L) stop("'sort' not yet implemented")
if (N > .Machine$integer.max) stop("no support for long vector in join-datagen yet")
N = as.integer(N)

datadir = "data"
if (!dir.exists(datadir)) stop(sprintf("directory '%s' does not exists", datadir))

# helper functions ----

# pretty print big numbers as 1e9, 1e8, etc
pretty_sci = function(x) {
  stopifnot(length(x)==1L, !is.na(x))
  tmp = strsplit(as.character(x), "+", fixed=TRUE)[[1L]]
  if (length(tmp)==1L) {
    paste0(substr(tmp, 1L, 1L), "e", nchar(tmp)-1L)
  } else if (length(tmp)==2L) {
    paste0(tmp[1L], as.character(as.integer(tmp[2L])))
  }
}
# ensure all values have been sampled
sample_all = function(unq_n, size) {
  stopifnot(unq_n <= size)
  unq_sub = seq_len(unq_n)
  sample(c(unq_sub, sample(unq_sub, size=max(size-unq_n, 0), replace=TRUE)))
}
# data validation
areInts = function(dt) {
  all(sapply(intersect(paste0("id", 1:3), names(dt)), function(col) is.integer(dt[[col]])))
}
# coerce to int
safe_add = function(x, y) {
  stopifnot(length(y)==1L)
  if (y > .Machine$integer.max) {
    if (!inherits(x, "integer64") || !inherits(y, "intege64")) {
      stop("for long vector support column and a value to add must be integer64 class")
    } else {
      stop("long vector support not yet implemented")
    }
  } else if (inherits(x, "integer")) {
    x + as.integer(y)
  } else {
    stop("column must be integer class, long vector is not yet supported")
  }
}
# data_name of table to join
join_to_tbls = function(data_name) {
  x_n = as.numeric(strsplit(data_name, "_", fixed=TRUE)[[1L]][2L])
  y_n = setNames(x_n/c(1e6, 1e3, 1e0), c("small","medium","big"))
  sapply(sapply(y_n, pretty_sci), gsub, pattern="NA", x=data_name)
}

# workhorse function ----

# generate RHS tables, re-reading from disk to reduce memory usage
y_gen = function(size, cols, y_data_name, dataf, exceptf, datadir) {
  cat(sprintf("Producing RHS data of %s rows\n", pretty_sci(size)))
  on = tail(cols, 1L)
  rhsf = tempfile(fileext="csv")
  # match data
  dt = setDT(readRDS(dataf))
  unq_on_join = sample(unique(dt[[on]]), size=max(trunc(size*0.9), 1), FALSE)
  dt_match = dt[.(unq_on_join), on=on, mult="first", cols, with=FALSE]
  rm(dt, unq_on_join)
  fwrite(dt_match, rhsf, showProgress=FALSE, append=FALSE)
  nr = nrow(dt_match)
  rm(dt_match)
  # nomatch data
  except = setDT(readRDS(exceptf))
  unq_on_except = sample(unique(except[[on]]), size=size-nr, FALSE)
  dt_nomatch = except[.(unq_on_except), on=on, mult="first", cols, with=FALSE]
  rm(except, unq_on_except, nr)
  fwrite(dt_nomatch, rhsf, showProgress=FALSE, append=TRUE)
  rm(dt_nomatch)
  # avoid rbindlist to reduce memory, instead fwrite append and fread
  y_dt = fread(rhsf, showProgress=FALSE, select=cols)
  stopifnot(areInts(y_dt), uniqueN(y_dt, by=on)==size)
  unlink(rhsf)
  # reorder randomly in place
  set(y_dt, NULL, "i", sample(nrow(y_dt)))
  setorderv(y_dt, "i")
  set(y_dt, NULL, "i", NULL)
  # add factor and measure variables
  if ("id1" %in% cols) set(y_dt, NULL, "id4", sprintf("id%.0f", y_dt$id1))
  if ("id2" %in% cols) set(y_dt, NULL, "id5", sprintf("id%.0f", y_dt$id2))
  if ("id3" %in% cols) set(y_dt, NULL, "id6", sprintf("id%.0f", y_dt$id3))
  set(y_dt, NULL, "v2", round(runif(nrow(y_dt), max=100), 6))
  # write RHS data to file
  yf = file.path(datadir, paste0(y_data_name, ".csv"))
  cat(sprintf("Writing RHS data to %s\n", yf))
  fwrite(y_dt, yf, showProgress=FALSE)
  rm(y_dt)
  invisible(TRUE)
}

# exec ----

library(data.table)
set.seed(108)
y_N = setNames(N/c(1e6, 1e3, 1e0), c("small","medium","big"))
data_name = sprintf("J1_%s_%s_%s_%s", pretty_sci(N), "NA", nas, sort)
# match data
cat(sprintf("Producing match data of %s rows\n", pretty_sci(N)))
DT = data.table(
  id1 = sample_all(N/1e6, N),
  id2 = sample_all(N/1e3, N),
  id3 = sample_all(N, N)
)
stopifnot(areInts(DT),
          uniqueN(DT, by="id1")==N/1e6,
          uniqueN(DT, by="id2")==N/1e3,
          uniqueN(DT, by="id3")==N)
dataf = tempfile(fileext="RDS")
saveRDS(DT, dataf)
# nomatch data
cat(sprintf("Producing nomatch data of %s rows\n", pretty_sci(N)))
# reorder randomly in place
set(DT, NULL, "i", sample(nrow(DT)))
setorderv(DT, "i")
set(DT, NULL, "i", NULL)
# increment id values so they will not match
set(DT, NULL, "id1", safe_add(DT$id1, y_N[["small"]]))
set(DT, NULL, "id2", safe_add(DT$id2, y_N[["medium"]]))
set(DT, NULL, "id3", safe_add(DT$id3, y_N[["big"]]))
stopifnot(areInts(DT),
          uniqueN(DT, by="id1")==N/1e6,
          uniqueN(DT, by="id2")==N/1e3,
          uniqueN(DT, by="id3")==N)
exceptf = tempfile(fileext="RDS")
saveRDS(DT, exceptf)
rm(DT)
# RHS data gen
mapply(y_gen, size = y_N,
       cols = list("id1", c("id1","id2"), c("id1","id2","id3")),
       y_data_name = join_to_tbls(data_name),
       MoreArgs = list(dataf=dataf, exceptf=exceptf, datadir=datadir)) -> nul
unlink(exceptf)
# LHS data finish
cat(sprintf("Producing LHS data of %s rows\n", pretty_sci(N)))
DT = setDT(readRDS(dataf))
unlink(dataf)
# add factor and measure variables
set(DT, NULL, "id4", sprintf("id%.0f", DT$id1))
set(DT, NULL, "id5", sprintf("id%.0f", DT$id2))
set(DT, NULL, "id6", sprintf("id%.0f", DT$id3))
set(DT, NULL, "v1", round(runif(nrow(DT), max=100), 6))
# write RHS data to file
file = file.path(datadir, paste0(data_name, ".csv"))
cat(sprintf("Writing LHS data to %s\n", file))
fwrite(DT, file, showProgress=FALSE)
rm(DT)
cat(sprintf("Join datagen of %s rows finished in %ss\n", pretty_sci(N), trunc(proc.time()[["elapsed"]]-init)))
if (!interactive()) quit("no", status=0)
