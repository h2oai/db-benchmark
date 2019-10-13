# Rscript join-datagen.R 1e7 0 0 0 ## 1e7 rows, 0 ignored, 0% NAs, random order
# Rscript join-datagen.R 1e8 0 5 1 ## 1e8 rows, 0 ignored, 5% NAs, sorted order

# see h2oai/db-benchmark#106 for a design notes of this procedure, feedback welcome in the issue

# init ----

args = commandArgs(TRUE)
N=as.integer(args[1L]); K=as.integer(args[2L]); nas=as.integer(args[3L]); sort=as.integer(args[4L])
#N=1e6L; K=NA_integer_; nas=0L; sort=0L
stopifnot(nas<=100L, nas>=0L, sort%in%c(0L,1L))
if (nas>0L) stop("'NA' not yet implemented")
if (sort==1L) stop("'sort' not yet implemented")

datadir = "data"
if (!dir.exists(datadir)) stop(sprintf("directory '%s' does not exists", datadir))

# helper functions ----

# pretty print big numbers as 1e9, 1e8, etc
pretty_sci = function(x) {
  stopifnot(length(x)==1L, !is.na(x))
  tmp = strsplit(as.character(x), "+", fixed=TRUE)[[1L]]
  if (length(tmp)==1L) {
    paste0(substr(tmp, 1L, 1L), "e", nchar(tmp)-1L)
  } else if(length(tmp)==2L) {
    paste0(tmp[1L], as.character(as.integer(tmp[2L])))
  }
}
# sample integer unsure unique according to provided ratio
sample_unq = function(N, ratio, size) {
  stopifnot(N > 0L, ratio <= 1, ratio > 0)
  unq_n = max(as.integer(N*ratio), 1L)
  unq_sub = sample(N, size=unq_n)
  sample(c(unq_sub, sample(unq_sub, size=max(size-unq_n, 0L), replace=TRUE)))
}

# data ----

library(data.table)
set.seed(108)
cat(sprintf("Producing data of %s rows\n", pretty_sci(N)))

DT = data.table(
  id1 = sample_unq(N/1e6, 1, N),
  id2 = sample_unq(N/1e3, 1, N),
  id3 = sample_unq(N, 1, N)
)
y_N = setNames(c(N/1e6, N/1e3, N), c("small","medium","big"))
DT_except = DT[sample(.N), mapply(`+`, .SD, y_N, SIMPLIFY=FALSE)]

all_levels = sprintf("id%011.0f", 1:(2L*N)) # char size fixed up to 1e11-1
DT[, `:=`(
  id4 = all_levels[sample(id1)],
  id5 = all_levels[sample(id2)],
  id6 = all_levels[sample(id3)]
)]
DT_except[, `:=`(
  id4 = all_levels[sample(id1)],
  id5 = all_levels[sample(id2)],
  id6 = all_levels[sample(id3)]
)]
rm(all_levels)

data_name = sprintf("J1_%s_%s_%s_%s", pretty_sci(N), "NA", nas, sort)

cat(sprintf("Producing join tables of %s rows\n", paste(collapse=", ", sapply(y_N, pretty_sci))))
y_gen = function(dt, except, size, on, cols) {
  unq_on_join = sample(unique(dt[[on]]), max(as.integer(size*0.9), 1L), FALSE)
  unq_on_except = sample(unique(except[[on]]), size-length(unq_on_join), FALSE)
  rbindlist(list(
    dt[.(unq_on_join), on=on, mult="first", cols, with=FALSE],
    except[.(unq_on_except), on=on, mult="first", cols, with=FALSE]
  ))[sample(.N)][, "v2" := round(runif(.N, max=100), 6)]
}
y_DT = list(
  small = y_gen(DT, DT_except, size=y_N[["small"]], on="id1", cols=c("id1","id4")),
  medium = y_gen(DT, DT_except, size=y_N[["medium"]], on="id2", cols=c("id1","id2","id4","id5")),
  big = y_gen(DT, DT_except, size=y_N[["big"]], on="id3", cols=c("id1","id2","id3","id4","id5","id6"))
)
DT[, "v1" := round(runif(.N, max=100), 6)]

# data_name of table to join
join_to_tbls = function(data_name) {
  x_n = as.numeric(strsplit(data_name, "_", fixed=TRUE)[[1L]][2L])
  y_n = setNames(c(x_n/1e6, x_n/1e3, x_n), c("small","medium","big"))
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
