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
if (sort==1L) stop("'sort' not yet implemented")
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
# finish RHS data by adding factor columns and measure and writing to file
do_rhs = function(r, data_name, datadir) {
	cols = names(r)
	if ("id1" %in% cols) set(r, NULL, "id4", sprintf("id%.0f", r$id1))
	if ("id2" %in% cols) set(r, NULL, "id5", sprintf("id%.0f", r$id2))
	if ("id3" %in% cols) set(r, NULL, "id6", sprintf("id%.0f", r$id3))
	set(r, NULL, "v2", round(runif(nrow(r), max=100), 6))
	f = file.path(datadir, paste0(data_name, ".csv"))
	cat(sprintf("Writing RHS data to %s\n", f))
	fwrite(r, f, showProgress=FALSE)
	invisible(TRUE)
}

# exec ----

library(data.table)
setDTthreads(0L)
set.seed(108)
y_N = setNames(N/c(1e6, 1e3, 1e0), c("small","medium","big"))
data_name = sprintf("J1_%s_%s_%s_%s", pretty_sci(N), "NA", nas, sort)
cat(sprintf("Generate join data of %s rows\n", pretty_sci(N)))
cat("Producing keys for LHS and RHS data\n")
key1 = split_xlr(N/1e6)
key2 = split_xlr(N/1e3)
key3 = split_xlr(N)
lhs = c("x","l")
l = data.table(
	id1 = sample_all(unlist(key1[lhs], use.names=FALSE), N),
	id2 = sample_all(unlist(key2[lhs], use.names=FALSE), N),
	id3 = sample_all(unlist(key3[lhs], use.names=FALSE), N)
)
stopifnot(
	uniqueN(l, by="id1")==N/1e6,
	uniqueN(l, by="id2")==N/1e3,
	uniqueN(l, by="id3")==N
)
rhs = c("x","r")
n = N/1e6
r1 = data.table(
	id1 = sample_all(unlist(key1[rhs], use.names=FALSE), n)
)
stopifnot(
	uniqueN(r1, by="id1")==n,
	l[r1, on="id1", round(.N/nrow(l), 2), nomatch=NULL]==0.9
)
n = N/1e3
r2 = data.table(
	id1 = sample_all(unlist(key1[rhs], use.names=FALSE), n),
	id2 = sample_all(unlist(key2[rhs], use.names=FALSE), n)
)
stopifnot(
	uniqueN(r2, by="id2")==n,
	l[r2, on="id2", round(.N/nrow(l), 2), nomatch=NULL]==0.9
)
n = N
r3 = data.table(
	id1 = sample_all(unlist(key1[rhs], use.names=FALSE), n),
	id2 = sample_all(unlist(key2[rhs], use.names=FALSE), n),
	id3 = sample_all(unlist(key3[rhs], use.names=FALSE), n)
)
stopifnot(
	uniqueN(r3, by="id3")==n,
  l[r3, on="id3", round(.N/nrow(l), 2), nomatch=NULL]==0.9
)
## for advanced questions
#l[r2, on=c("id1","id2"), nomatch=NULL]
#l[r2, on=.(id1>id1,id2), nomatch=NULL]

cat("Producing LHS data from keys\n")
set(l, NULL, "id4", sprintf("id%.0f", l$id1)) # add factor and measure variables
set(l, NULL, "id5", sprintf("id%.0f", l$id2))
set(l, NULL, "id6", sprintf("id%.0f", l$id3))
set(l, NULL, "v1", round(runif(nrow(l), max=100), 6))
f = file.path(datadir, paste0(data_name, ".csv")) # write LHS data to file
cat(sprintf("Writing LHS data to %s\n", f))
fwrite(l, f, showProgress=FALSE)
rm(l, f)

r_data_name = join_to_tbls(data_name)
do_rhs(r1, r_data_name[1L], datadir)
rm(r1)
do_rhs(r2, r_data_name[2L], datadir)
rm(r2)
do_rhs(r3, r_data_name[3L], datadir)
rm(r3)

cat(sprintf("Join datagen of %s rows finished in %ss\n", pretty_sci(N), trunc(proc.time()[["elapsed"]]-init)))
if (!interactive()) quit("no", status=0)
