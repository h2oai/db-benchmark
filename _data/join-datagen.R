# Rscript join-datagen.R 1e7 0 0 0 ## 1e7 rows, 0 ignored, 0% NAs, random order
# Rscript join-datagen.R 1e8 0 5 1 ## 1e8 rows, 0 ignored, 5% NAs, sorted order

# see h2oai/db-benchmark#106 for a design notes of this procedure, feedback welcome in the issue

# init ----

init = proc.time()[["elapsed"]]
args = commandArgs(TRUE)
N=as.numeric(args[1L]); K=as.integer(args[2L]); nas=as.integer(args[3L]); sort=as.integer(args[4L])
#N=1e7; K=NA_integer_; nas=0L; sort=0L
stopifnot(N>=1e7, nas<=100L, nas>=0L, sort%in%c(0L,1L))
if (nas>0L) stop("'NA' not yet implemented")
if (N > .Machine$integer.max) stop("no support for long vector in join-datagen yet")
N = as.integer(N)

datadir = "." ## this is meant to be called from data directory as Rscript ../_data/join-datagen.R so readers don't have to create etra directory to generate, same as for groupby data
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
# data_name of table to join
join_to_tbls = function(data_name) {
  x_n = as.numeric(strsplit(data_name, "_", fixed=TRUE)[[1L]][2L])
  y_n = setNames(x_n/c(1e6, 1e3, 1e0), c("small","medium","big"))
  sapply(sapply(y_n, pretty_sci), gsub, pattern="NA", x=data_name)
}
# sample ensuring none is missing
sample_all = function(x, size) {
  stopifnot(length(x) <= size)
  y = c(x, sample(x, size=max(size-length(x), 0), replace=TRUE))
  sample(y)
}
# split into common (0.9) left (0.1) and right (0.1)
split_xlr = function(n) {
  key = sample.int(n*1.1) # 1.1 = 0.9+0.1+0.1
  list(
    x = key[seq.int(1, n*0.9)],
    l = key[seq.int(n*0.9+1, n)],
    r = key[seq.int(n+1, n*1.1)]
  )
}
# we need to write in batches to reduce memory footprint
write_batches = function(d, name, datadir, append) {
  cols = names(d)
  if ("id1" %in% cols) set(d, NULL, "id4", sprintf("id%.0f", d$id1))
  if ("id2" %in% cols) set(d, NULL, "id5", sprintf("id%.0f", d$id2))
  if ("id3" %in% cols) set(d, NULL, "id6", sprintf("id%.0f", d$id3))
  setcolorder(d, neworder=setdiff(names(d), c("v1","v2")))
  f = file.path(datadir, paste0(name, ".csv"))
  fwrite(d, f, showProgress=FALSE, append=append)
}
handle_batches = function(d, data_name, datadir) {
  N = nrow(d)
  if (N > 1e8) {
    stopifnot(N==1e9)
    for (i in 1:10) {
      cat(sprintf("Writing %s data batch %s\n", pretty_sci(N), i))
      write_batches(d[((i-1)*1e8+1L):(i*1e8)], data_name, datadir, append=i>1L)
    }
  } else {
    write_batches(d, data_name, datadir, append=FALSE)
  }
}

# exec ----

library(data.table)
setDTthreads(0L)
set.seed(108)
data_name = sprintf("J1_%s_%s_%s_%s", pretty_sci(N), "NA", nas, sort)

cat(sprintf("Generate join data of %s rows\n", pretty_sci(N)))

cat("Producing keys for LHS and RHS data\n")
key1 = split_xlr(N/1e6)
key2 = split_xlr(N/1e3)
key3 = split_xlr(N)

cat(sprintf("Producing LHS %s data from keys\n", N))
lhs = c("x","l")
l = list(
  id1 = sample_all(unlist(key1[lhs], use.names=FALSE), N),
  id2 = sample_all(unlist(key2[lhs], use.names=FALSE), N),
  id3 = sample_all(unlist(key3[lhs], use.names=FALSE), N)
)
setDT(l)
if (sort==1L) {
  cat("Sorting LHS data\n")
  setkeyv(l, c("id1","id2","id3"))
}
set(l, NULL, "v1", round(runif(nrow(l), max=100), 6))
stopifnot(
  uniqueN(l, by="id1")==N/1e6,
  uniqueN(l, by="id2")==N/1e3,
  uniqueN(l, by="id3")==N
)
cat(sprintf("Writing LHS %s data %s\n", N, data_name))
handle_batches(l, data_name, datadir)
rm(l)

rhs = c("x","r")
r_data_name = join_to_tbls(data_name)
n = N/1e6
cat(sprintf("Producing RHS %s data from keys\n", n))
r1 = list(
  id1 = sample_all(unlist(key1[rhs], use.names=FALSE), n)
)
setDT(r1)
if (sort==1L) {
  cat("Sorting RHS small data\n")
  setkeyv(r1, "id1")
}
set(r1, NULL, "v2", round(runif(nrow(r1), max=100), 6))
stopifnot(uniqueN(r1, by="id1")==n)
cat(sprintf("Writing RHS %s data %s\n", n, r_data_name[1L]))
handle_batches(r1, r_data_name[1L], datadir)
rm(r1)
n = N/1e3
cat(sprintf("Producing RHS %s data from keys\n", n))
r2 = list(
  id1 = sample_all(unlist(key1[rhs], use.names=FALSE), n),
  id2 = sample_all(unlist(key2[rhs], use.names=FALSE), n)
)
setDT(r2)
if (sort==1L) {
  cat("Sorting RHS medium data\n")
  setkeyv(r2, "id2")
}
set(r2, NULL, "v2", round(runif(nrow(r2), max=100), 6))
stopifnot(uniqueN(r2, by="id2")==n)
cat(sprintf("Writing RHS %s data %s\n", n, r_data_name[2L]))
handle_batches(r2, r_data_name[2L], datadir)
rm(r2)
n = N
cat(sprintf("Producing RHS %s data from keys\n", n))
r3 = list(
  id1 = sample_all(unlist(key1[rhs], use.names=FALSE), n),
  id2 = sample_all(unlist(key2[rhs], use.names=FALSE), n),
  id3 = sample_all(unlist(key3[rhs], use.names=FALSE), n)
)
rm(key1, key2, key3)
setDT(r3)
if (sort==1L) {
  cat("Sorting RHS big data\n")
  setkeyv(r3, "id3")
}
set(r3, NULL, "v2", round(runif(nrow(r3), max=100), 6))
stopifnot(uniqueN(r3, by="id3")==n)
cat(sprintf("Writing RHS %s data %s\n", n, r_data_name[3L]))
handle_batches(r3, r_data_name[3L], datadir)
rm(r3)

cat(sprintf("Join datagen of %s rows finished in %ss\n", pretty_sci(N), trunc(proc.time()[["elapsed"]]-init)))
if (!interactive()) quit("no", status=0)
