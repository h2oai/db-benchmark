#!/usr/bin/env Rscript

cat("# groupby-duckdb.R\n")

source("./_helpers/helpers.R")

suppressPackageStartupMessages({
  library("DBI", lib.loc="./duckdb/r-duckdb", warn.conflicts=FALSE)
  library("duckdb", lib.loc="./duckdb/r-duckdb", warn.conflicts=FALSE)
})
ver = packageVersion("duckdb")
#git = "" # set up later on after connecting to db
task = "groupby"
solution = "duckdb"
fun = "group_by"
cache = TRUE
on_disk = FALSE

# we could also do an on-disk version if you like ^^
# just pass tempfile() to dbConnect() below

data_name = Sys.getenv("SRC_DATANAME")
src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

con = dbConnect(duckdb::duckdb())

ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
git = dbGetQuery(con, "SELECT source_id FROM pragma_version()")[[1L]]

invisible(dbExecute(con, sprintf("CREATE TABLE x AS SELECT * FROM READ_CSV_AUTO('%s')", src_grp)))
print(in_nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM x")$cnt)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

task_init = proc.time()[["elapsed"]]
cat("grouping...\n")

question = "sum v1 by id1" # q1
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id1, SUM(v1) AS v1 FROM x GROUP BY id1")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id1, SUM(v1) AS v1 FROM x GROUP BY id1")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT COUNT(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "sum v1 by id1:id2" # q2
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id1, id2, SUM(v1) AS v1 FROM x GROUP BY id1, id2")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id1, id2, SUM(v1) AS v1 FROM x GROUP BY id1, id2")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT COUNT(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "sum v1 mean v3 by id3" # q3
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM x GROUP BY id3")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v3) AS v3 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM x GROUP BY id3")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v3) AS v3 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT COUNT(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "mean v1:v3 by id4" # q4
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM x GROUP BY id4")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM x GROUP BY id4")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT COUNT(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "sum v1:v3 by id6" # q5
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM x GROUP BY id6")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM x GROUP BY id6")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT COUNT(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "median v3 sd v3 by id4 id5" # q6
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id4, id5, MEDIAN(v3) AS median_v3, STDDEV(v3) AS sd_v3 FROM x GROUP BY id4, id5")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(median_v3) AS median_v3, SUM(sd_v3) AS sd_v3 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id4, id5, MEDIAN(v3) AS median_v3, STDDEV(v3) AS sd_v3 FROM x GROUP BY id4, id5")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(median_v3) AS median_v3, SUM(sd_v3) AS sd_v3 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT COUNT(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "max v1 - min v2 by id3" # q7
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id3, MAX(v1)-MIN(v2) AS range_v1_v2 FROM x GROUP BY id3")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(range_v1_v2) AS range_v1_v2 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id3, MAX(v1)-MIN(v2) AS range_v1_v2 FROM x GROUP BY id3")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(range_v1_v2) AS range_v1_v2 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT COUNT(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "largest two v3 by id6" # q8
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id6, v3 AS largest2_v3 FROM (SELECT id6, v3, ROW_NUMBER() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS order_v3 FROM x WHERE v3 IS NOT NULL) sub_query WHERE order_v3 <= 2")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(largest2_v3) AS largest2_v3 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id6, v3 AS largest2_v3 FROM (SELECT id6, v3, ROW_NUMBER() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS order_v3 FROM x WHERE v3 IS NOT NULL) sub_query WHERE order_v3 <= 2")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(largest2_v3) AS largest2_v3 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT COUNT(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "regression v1 v2 by id2 id4" # q9
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(r2) AS r2 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(r2) AS r2 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT COUNT(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

question = "sum v3 count by id1:id6" # q10
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id1, id2, id3, id4, id5, id6, SUM(v3) AS v3, COUNT(*) AS count FROM x GROUP BY id1, id2, id3, id4, id5, id6")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v3) AS v3, SUM(count) AS count FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, "CREATE TABLE ans AS SELECT id1, id2, id3, id4, id5, id6, SUM(v3) AS v3, COUNT(*) AS count FROM x GROUP BY id1, id2, id3, id4, id5, id6")
  print(c(nr<-dbGetQuery(con, "SELECT COUNT(*) cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-dbGetQuery(con, "SELECT SUM(v3) AS v3, SUM(count) AS count FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT COUNT(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

cat(sprintf("grouping finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
