#!/usr/bin/env Rscript

cat("# join-dplyr.R\n")

source("./helpers.R")
source("./dplyr/helpers-dplyr.R")

src_x = Sys.getenv("SRC_X", NA_character_)
src_y = Sys.getenv("SRC_Y", NA_character_)

if (get.nrow(c(src_x,src_y)) > 2e9L) {
  cat("# join with dplyr skipped due data volume cap for single machine set to total 2e9 rows")
  quit("no", status=0) # datasets > 1e9 too big to try load on single machine
}

stopifnot(requireNamespace("bit64", quietly=TRUE)) # used in chk to sum numeric columns
suppressPackageStartupMessages(library(dplyr, warn.conflicts=FALSE))
ver = packageVersion("dplyr")
git = dplyr.git()
cat("loading datasets...\n")
X = data.table::fread(if(file.exists(basename(src_x))) basename(src_x) else sprintf("hadoop fs -cat %s", src_x), data.table=FALSE) # csv can be provided in local dir for faster import
Y = data.table::fread(if(file.exists(basename(src_y))) basename(src_y) else sprintf("hadoop fs -cat %s", src_y), data.table=FALSE)
data_name = paste(basename(src_x), basename(src_y), sep="-")
task = "join"
solution = "dplyr"
fun = "inner_join"
question = "inner join"
cache = TRUE

cat("joining...\n")

t = system.time(print(dim(ans<-inner_join(X, Y, by="KEY"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(bit64::as.integer64(X2)), sum(bit64::as.integer64(Y2))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

t = system.time(print(dim(ans<-inner_join(X, Y, by="KEY"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(bit64::as.integer64(X2)), sum(bit64::as.integer64(Y2))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

t = system.time(print(dim(ans<-inner_join(X, Y, by="KEY"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(bit64::as.integer64(X2)), sum(bit64::as.integer64(Y2))))[["elapsed"]]
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

if( !interactive() ) q("no", status=0)
