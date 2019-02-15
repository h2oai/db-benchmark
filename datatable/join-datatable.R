#!/usr/bin/env Rscript

cat("# join-datatable.R\n")

source("./helpers.R")
source("./datatable/helpers-datatable.R")

stopifnot(requireNamespace(c("bit64"), quietly=TRUE)) # used in chk to sum numeric columns
suppressPackageStartupMessages(library(data.table))
ver = packageVersion("data.table")
git = datatable.git()
task = "join"
solution = "data.table"
fun = "[.data.table"
cache = TRUE

data_name = Sys.getenv("SRC_JN_LOCAL") # "J1_1e6_NA_0_0"
src_jn_x = file.path("data", paste(data_name, "csv", sep="."))
y_data_name = sapply((function(x) sapply(setNames(c(x, x/1e3, x/1e6), c("big","medium","small")), pretty_sci))(as.numeric(strsplit(data_name, "_", fixed=TRUE)[[1L]][2L])), gsub, pattern="NA", x=data_name)
src_jn_y = setNames(file.path("data", paste(y_data_name, "csv", sep=".")), names(y_data_name))
stopifnot(length(src_jn_y)==3L)
cat(sprintf("loading datasets %s\n", paste(c(data_name, y_data_name), collapse=", ")))

DT = fread(src_jn_x, showProgress=FALSE, stringsAsFactors=TRUE)
JN = sapply(simplify=FALSE, src_jn_y, fread, showProgress=FALSE, stringsAsFactors=TRUE)
print(nrow(DT))
sapply(sapply(JN, nrow), print) -> nul

cat("joining...\n")

#inner, singlecol, integer, big-big
#inner, singlecol, integer, big-medium
#inner, singlecol, integer, big-small

#outer, singlecol, integer, big-medium

#inner, singlecol, factor, big-medium

#inner, multicol, integer, big-medium

#inner, singlecol, integer, big-medium, update on join

question = "big inner join on unique int" # q1
t = system.time(print(dim(ans<-DT[JN$big, on="id1", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(X2)), sum(bit64::as.integer64(Y2)))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)
t = system.time(print(dim(ans<-X[Y, on="KEY", nomatch=NULL])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(X2)), sum(bit64::as.integer64(Y2)))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

if( !interactive() ) q("no", status=0)
