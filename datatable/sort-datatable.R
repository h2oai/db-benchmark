#!/usr/bin/env Rscript

cat("# sort-datatable.R\n")

source("./helpers.R")
source("./datatable/helpers-datatable.R")

src_x = Sys.getenv("SRC_X", NA_character_)

# if (get.nrow(src_x) > 1e9L) {
#   cat("# sort with data.table skipped due data volume cap for single machine set to total 1e9 rows")
#   quit("no", status=0) # datasets > 1e9 too big to try load on single machine
# }

stopifnot(requireNamespace("bit64", quietly=TRUE)) # used in chk to sum numeric columns
suppressPackageStartupMessages(library(data.table))
ver = packageVersion("data.table")
git = datatable.git()
data_name = basename(src_x)
task = "sort"
solution = "data.table"
fun = "[.data.table"
question = "by int KEY"
cache = TRUE

cat("loading dataset...\n")
X = fread(if(file.exists(basename(src_x))) basename(src_x) else sprintf("hadoop fs -cat %s", src_x)) # csv can be provided in local dir for faster import

cat("sorting...\n")
t = system.time(print(dim(ans<-X[order(KEY)])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(X2)))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

t = system.time(print(dim(ans<-X[order(KEY)])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(X2)))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

t = system.time(print(dim(ans<-X[order(KEY)])))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(X2)))])[["elapsed"]]
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

if( !interactive() ) q("no", status=0)
