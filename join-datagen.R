# Rscript join-datagen.R 1e7 0 0 0 ## 1e7 rows, 0 ignored, 0% NAs, random order
# Rscript join-datagen.R 1e8 0 5 1 ## 1e8 rows, 0 ignored, 5% NAs, sorted order

# see h2oai/db-benchmark#106 for a design notes of this procedure, feedback welcome in the issue

# init ----

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
# mem efficient to replace DT[sample(.N)], till data.table#4012
setroworder = function(x, neworder) {
    .Call(data.table:::Creorder, x, as.integer(neworder), PACKAGE="data.table")
    invisible(x)
}

# data ----

library(data.table)
set.seed(108)
cat(sprintf("Producing data of %s rows\n", pretty_sci(N)))

DT = data.table(
  id1 = sample_all(N/1e6, N),
  id2 = sample_all(N/1e3, N),
  id3 = sample_all(N, N)
)
stopifnot(areInts(DT))
y_N = setNames(N/c(1e6, 1e3, 1e0), c("small","medium","big"))
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
DT_except = DT[sample(.N), mapply(safe_add, .SD, y_N, SIMPLIFY=FALSE)]
stopifnot(areInts(DT_except))
stopifnot(uniqueN(DT, by="id1")==N/1e6,
          uniqueN(DT, by="id2")==N/1e3,
          uniqueN(DT, by="id3")==N,
          uniqueN(DT_except, by="id1")==N/1e6,
          uniqueN(DT_except, by="id2")==N/1e3,
          uniqueN(DT_except, by="id3")==N)

all_levels = sprintf("id%.0f", 1:(2*N))
DT[, `:=`(
  id4 = all_levels[id1],
  id5 = all_levels[id2],
  id6 = all_levels[id3]
)]
DT_except[, `:=`(
  id4 = all_levels[id1],
  id5 = all_levels[id2],
  id6 = all_levels[id3]
)]
rm(all_levels)
stopifnot(uniqueN(DT, by="id4")==N/1e6,
          uniqueN(DT, by="id5")==N/1e3,
          uniqueN(DT, by="id6")==N,
          uniqueN(DT_except, by="id4")==N/1e6,
          uniqueN(DT_except, by="id5")==N/1e3,
          uniqueN(DT_except, by="id6")==N)

data_name = sprintf("J1_%s_%s_%s_%s", pretty_sci(N), "NA", nas, sort)

cat(sprintf("Producing join tables of %s rows\n", paste(collapse=", ", sapply(y_N, pretty_sci))))
y_gen = function(dt, except, size, on, cols) {
  unq_on_join = sample(unique(dt[[on]]), size=max(as.integer(size*0.9), 1), FALSE)
  unq_on_except = sample(unique(except[[on]]), size=size-length(unq_on_join), FALSE)
  y_dt = rbindlist(list(
    dt[.(unq_on_join), on=on, mult="first", cols, with=FALSE],
    except[.(unq_on_except), on=on, mult="first", cols, with=FALSE]
  ))
  setroworder(y_dt, neworder=sample(nrow(y_dt)))
  y_dt[, "v2" := round(runif(.N, max=100), 6)]
}
y_DT = list(
  small = y_gen(DT, DT_except, size=y_N[["small"]], on="id1", cols=c("id1","id4")),
  medium = y_gen(DT, DT_except, size=y_N[["medium"]], on="id2", cols=c("id1","id2","id4","id5")),
  big = z<-y_gen(DT, DT_except, size=y_N[["big"]], on="id3", cols=c("id1","id2","id3","id4","id5","id6"))
)
stopifnot(sapply(y_DT, areInts))
stopifnot(uniqueN(y_DT$small, by="id1")==N/1e6, uniqueN(y_DT$small, by="id4")==N/1e6,
          uniqueN(y_DT$medium, by="id2")==N/1e3, uniqueN(y_DT$medium, by="id5")==N/1e3,
          uniqueN(y_DT$big, by="id3")==N, uniqueN(y_DT$big, by="id6")==N)
DT[, "v1" := round(runif(.N, max=100), 6)]

# data_name of table to join
join_to_tbls = function(data_name) {
  x_n = as.numeric(strsplit(data_name, "_", fixed=TRUE)[[1L]][2L])
  y_n = setNames(x_n/c(1e6, 1e3, 1e0), c("small","medium","big"))
  sapply(sapply(y_n, pretty_sci), gsub, pattern="NA", x=data_name)
}
y_data_name = join_to_tbls(data_name)

if (nas>0L) {
  stop("internal error: 'NA' not yet implemented")
}
if (sort==1L) {
  stop("internal error: 'sort' not yet implemented")
}
file = file.path(datadir, paste0(data_name, ".csv"))
cat(sprintf("Writing data to %s\n", file))
fwrite(DT, file)
y_file = file.path(datadir, paste0(y_data_name, ".csv"))
mapply(function(DT, file) fwrite(DT, file), y_DT, y_file) -> nul
cat(sprintf("Data written to %s, quitting\n", paste(y_file, collapse=", ")))

if (!interactive()) quit("no", status=0)
