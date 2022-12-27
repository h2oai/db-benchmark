#!/usr/bin/env Rscript

cat("# join-duckdb.R\n")

source("./_helpers/helpers.R")

suppressPackageStartupMessages({
  library("DBI", lib.loc="./duckdb/r-duckdb", warn.conflicts=FALSE)
  library("duckdb", lib.loc="./duckdb/r-duckdb", warn.conflicts=FALSE)
})
ver = packageVersion("duckdb")
#git = "" # set up later on after connecting to db
task = "join"
solution = "duckdb"
cache = TRUE

data_name = Sys.getenv("SRC_DATANAME")
src_jn_x = file.path("data", paste(data_name, "csv", sep="."))
y_data_name = join_to_tbls(data_name)
src_jn_y = setNames(file.path("data", paste(y_data_name, "csv", sep=".")), names(y_data_name))
stopifnot(length(src_jn_y)==3L)
cat(sprintf("loading datasets %s\n", paste(c(data_name, y_data_name), collapse=", ")))

on_disk = as.numeric(strsplit(data_name, "_", fixed=TRUE)[[1L]][2L])>=1e9
uses_NAs = as.numeric(strsplit(data_name, "_", fixed=TRUE)[[1L]][4L])>0
if (on_disk) {
  print("using disk memory-mapped data storage")
  con = dbConnect(duckdb::duckdb(), dbdir=tempfile())
} else {
  print("using in-memory data storage")
  con = dbConnect(duckdb::duckdb())
}

ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
git = dbGetQuery(con, "SELECT source_id FROM pragma_version()")[[1L]]


if (!uses_NAs) {
  invisible({
    dbExecute(con, sprintf("CREATE TYPE id4ENUM AS ENUM (SELECT distinct(id4) FROM (select distinct(id4) from read_csv_auto('%s') UNION ALL select distinct(id4) FROM read_csv_auto('%s') UNION ALL select distinct(id4) from read_csv_auto('%s') UNION ALL select distinct(id4) from read_csv_auto('%s')))", src_jn_x, src_jn_y[1L], src_jn_y[2L], src_jn_y[3L]))
    dbExecute(con, sprintf("CREATE TYPE id5ENUM AS ENUM (SELECT distinct(id5) FROM (select distinct(id5) FROM read_csv_auto('%s') UNION ALL select distinct(id5) from read_csv_auto('%s') UNION ALL select distinct(id5) from read_csv_auto('%s')))", src_jn_x, src_jn_y[2L], src_jn_y[3L]))
    dbExecute(con, sprintf("CREATE TYPE id6ENUM AS ENUM (SELECT distinct(id6) FROM (select distinct(id6) from read_csv_auto('%s') UNION ALL select distinct(id6) from read_csv_auto('%s')))", src_jn_x, src_jn_y[3L]))

    dbExecute(con, "CREATE TABLE x(id1 INT, id2 INT, id3 INT, id4 id4ENUM, id5 id5ENUM, id6 id6ENUM, v1 FLOAT)")
    dbExecute(con, sprintf("COPY x FROM '%s' (AUTO_DETECT TRUE)", src_jn_x))

    dbExecute(con, "CREATE TABLE small(id1 INT64, id4 id4ENUM, v2 FLOAT)")
    dbExecute(con, sprintf("copy small FROM '%s' (AUTO_DETECT TRUE)", src_jn_y[1L]))

    dbExecute(con, "CREATE TABLE medium(id1 INT, id2 INT, id4 id4ENUM, id5 id5ENUM, v2 FLOAT)")
    dbExecute(con, sprintf("copy medium FROM '%s' (AUTO_DETECT TRUE)", src_jn_y[2L]))

    dbExecute(con, "CREATE TABLE big(id1 INT, id2 INT, id3 INT, id4 id4ENUM, id5 id5ENUM, id6 id6ENUM, v2 FLOAT)")
    dbExecute(con, sprintf("Copy big FROM '%s' (AUTO_DETECT TRUE)", src_jn_y[3L]))
  })
} else {
  invisible({
    dbExecute(con, "CREATE TABLE x(id1 INT, id2 INT, id3 INT, id4 VARCHAR, id5 VARCHAR, id6 VARCHAR, v1 FLOAT)")
    dbExecute(con, sprintf("COPY x FROM '%s' (AUTO_DETECT TRUE)", src_jn_x))

    dbExecute(con, "CREATE TABLE small(id1 INT64, id4 VARCHAR, v2 FLOAT)")
    dbExecute(con, sprintf("copy small FROM '%s' (AUTO_DETECT TRUE)", src_jn_y[1L]))

    dbExecute(con, "CREATE TABLE medium(id1 INT, id2 INT, id4 VARCHAR, id5 VARCHAR, v2 FLOAT)")
    dbExecute(con, sprintf("copy medium FROM '%s' (AUTO_DETECT TRUE)", src_jn_y[2L]))

    dbExecute(con, "CREATE TABLE big(id1 INT, id2 INT, id3 INT, id4 VARCHAR, id5 VARCHAR, id6 VARCHAR, v2 FLOAT)")
    dbExecute(con, sprintf("Copy big FROM '%s' (AUTO_DETECT TRUE)", src_jn_y[3L]))
  })
}

print(in_nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM x")$cnt)
print(dbGetQuery(con, "SELECT count(*) AS cnt FROM small")$cnt)
print(dbGetQuery(con, "SELECT count(*) AS cnt FROM medium")$cnt)
print(dbGetQuery(con, "SELECT count(*) AS cnt FROM big")$cnt)

task_init = proc.time()[["elapsed"]]
cat("joining...\n")

question = "small inner on int" # q1
fun = "inner_join"
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1)")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1)")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "medium inner on int" # q2
fun = "inner_join"
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x JOIN medium USING (id2)")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x JOIN medium USING (id2)")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "medium outer on int" # q3
fun = "left_join"
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x LEFT JOIN medium USING (id2)")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x LEFT JOIN medium USING (id2)")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "medium inner on factor" # q4
fun = "inner_join"
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 FROM x JOIN medium USING (id5)")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 FROM x JOIN medium USING (id5)")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "big inner on int" # q5
fun = "inner_join"
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x JOIN big USING (id3)")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x JOIN big USING (id3)")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

cat(sprintf("joining finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
