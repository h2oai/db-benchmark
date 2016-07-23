#!/usr/bin/env Rscript

cat("# groupby-datatable.R\n")

source("./helpers.R")
source("./datatable/helpers-datatable.R")

library(data.table)
ver = packageVersion("data.table")
task = "groupby"
solution = "data.table"
fun = "[.data.table"

cat("loading dataset\n")
data_name = "G1_1e7_1e2.csv"
# if (get.nrow(c(DT)) > 1e9L) {
#   cat("# groupby with data.table skipped due data volume cap for single machine set to total 1e9 rows")
#   quit("no", status=0) # datasets > 1e9 too big to try load on single machine
# }
X = fread(data_name)

question = "sum v1 by id1" #1
t = system.time(dim(ans<-X[, sum(v1), by=id1]))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)
t = system.time(dim(ans<-X[, sum(v1), by=id1]))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)
t = system.time(dim(ans<-X[, sum(v1), by=id1]))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)

question = "sum v1 by id1:id2" #2
t = system.time(dim(ans<-X[, sum(v1), by=.(id1, id2)]))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)
t = system.time(dim(ans<-X[, sum(v1), by=.(id1, id2)]))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)
t = system.time(dim(ans<-X[, sum(v1), by=.(id1, id2)]))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)

question = "sum v1 mean v3 by id3" #3
t = system.time(dim(ans<-X[, .(sum(v1), mean(v3)), by=id3]))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)
t = system.time(dim(ans<-X[, .(sum(v1), mean(v3)), by=id3]))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)
t = system.time(dim(ans<-X[, .(sum(v1), mean(v3)), by=id3]))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)

question = "mean v1:v3 by id4" #4
t = system.time(dim(ans<-X[, lapply(.SD, mean), by=id4, .SDcols=v1:v3]))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)
t = system.time(dim(ans<-X[, lapply(.SD, mean), by=id4, .SDcols=v1:v3]))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)
t = system.time(dim(ans<-X[, lapply(.SD, mean), by=id4, .SDcols=v1:v3]))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)

question = "sum v1:v3 by id6" #5
t = system.time(dim(ans<-X[, lapply(.SD, sum), by=id6, .SDcols=v1:v3]))[["elapsed"]]
m = memory_usage()
write.log(run=1L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)
t = system.time(dim(ans<-X[, lapply(.SD, sum), by=id6, .SDcols=v1:v3]))[["elapsed"]]
m = memory_usage()
write.log(run=2L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)
t = system.time(dim(ans<-X[, lapply(.SD, sum), by=id6, .SDcols=v1:v3]))[["elapsed"]]
m = memory_usage()
write.log(run=3L, task=task, data=data_name, in_rows=nrow(X), question=question, out_rows=nrow(ans), solution=solution, version=ver, fun=fun, time_sec=t, mem_gb=m)
rm(ans)

if( !interactive() ) q("no", status=0)
