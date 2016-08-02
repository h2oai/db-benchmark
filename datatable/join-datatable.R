#!/usr/bin/env Rscript

cat("# join-datatable.R\n")

source("./helpers.R")
source("./datatable/helpers-datatable.R")

src_x = Sys.getenv("SRC_X", NA_character_)
src_y = Sys.getenv("SRC_Y", NA_character_)

if (get.nrow(c(src_x,src_y)) > 2e9L) {
  cat("# join with data.table skipped due data volume cap for single machine set to total 2e9 rows")
  quit("no", status=0) # datasets > 1e9 too big to try load on single machine
}

# library(bit64)
library(data.table)
ver = packageVersion("data.table")
git = datatable.git()
cat("loading datasets...\n")
X = fread(if(file.exists(src_x)) src_x else sprintf("hadoop fs -cat %s", src_x)) # csv can be provided in local dir for faster import
Y = fread(if(file.exists(src_y)) src_y else sprintf("hadoop fs -cat %s", src_y))
data_name = paste(basename(src_x), basename(src_y), sep="-")
task = "join"
solution = "data.table"
fun = "[.data.table"
question = "inner join"
cache = TRUE

cat("joining...\n")
t = system.time(dim(ans<-X[Y, on="KEY", nomatch=0L]))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(X2)), sum(bit64::as.integer64(Y2)))])[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

t = system.time(dim(ans<-X[Y, on="KEY", nomatch=0L]))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(X2)), sum(bit64::as.integer64(Y2)))])[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

t = system.time(dim(ans<-X[Y, on="KEY", nomatch=0L]))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans[, .(sum(bit64::as.integer64(X2)), sum(bit64::as.integer64(Y2)))])[["elapsed"]]
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
rm(ans)

if( !interactive() ) q("no", status=0)
