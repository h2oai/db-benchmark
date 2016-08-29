#!/usr/bin/env Rscript

cat("# sort-dplyr\n")

source("./helpers.R")
source("./dplyr/helpers-dplyr.R")

src_x = Sys.getenv("SRC_X", NA_character_)

# if (get.nrow(src_x) > 1e9L) {
#   cat("# sort with dplyr skipped due data volume cap for single machine set to total 1e9 rows")
#   quit("no", status=0) # datasets > 1e9 too big to try load on single machine
# }

library(dplyr, warn.conflicts=FALSE)
ver = packageVersion("dplyr")
git = NA_character_
data_name = basename(src_x)
task = "sort"
solution = "dplyr"
fun = "arrange"
question = "by int KEY"
cache = TRUE

cat("loading dataset...\n")
X = data.table::fread(if(file.exists(basename(src_x))) basename(src_x) else sprintf("hadoop fs -cat %s", src_x), data.table=FALSE) # csv can be provided in local dir for faster import

cat("sorting...\n")
t = system.time(dim(ans<-arrange(X, KEY)))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(bit64::as.integer64(X2))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

t = system.time(dim(ans<-arrange(X, KEY)))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(bit64::as.integer64(X2))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

t = system.time(dim(ans<-arrange(X, KEY)))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, sum(bit64::as.integer64(X2))))[["elapsed"]]
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

if( !interactive() ) q("no", status=0)
