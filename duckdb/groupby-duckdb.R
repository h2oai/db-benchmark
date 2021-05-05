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

stop("dev")

# globals galore but meh
duckdb_bench = function(query, check_query) {
	get_x_count   = function(con) dbGetQuery(con, "SELECT COUNT(*) c FROM x")$c
	get_ans_count = function(con) dbGetQuery(con, "SELECT COUNT(*) c FROM ans")$c
	get_ans_cols  = function(con) ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 1"))

	invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
	# run 1 
	t = system.time(invisible(dbExecute(con, sprintf("CREATE TABLE ans AS %s", query))))[["elapsed"]]
	m = memory_usage()
	chkt = system.time(chk <<- dbGetQuery(con, check_query)[[1]])[["elapsed"]]
	write.log(run=1L, task=task, data=data_name, in_rows=get_x_count(con), question=question, out_rows=get_ans_count(con), out_cols=get_ans_count(con), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
	invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
	# run 2
	t = system.time(invisible(dbExecute(con, sprintf("CREATE TABLE ans AS %s", query))))[["elapsed"]]
	m = memory_usage()
	chkt = system.time(chk <<- dbGetQuery(con, check_query)[[1]])[["elapsed"]]
	write.log(run=2L, task=task, data=data_name, in_rows=get_x_count(con), question=question, out_rows=get_ans_count(con), out_cols=get_ans_count(con), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
	print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))
	print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT COUNT(*) FROM ans) - 4"))
	invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
}

question = "sum v1 by id1" # q1
duckdb_bench("SELECT id1, SUM(v1) AS v1 FROM x GROUP BY id1", "SELECT SUM(v1) AS v1 FROM ans")

question = "sum v1 by id1:id2" # q2
duckdb_bench("SELECT id1, id2, SUM(v1) AS v1 FROM x GROUP by id1, id2", "SELECT SUM(v1) AS v1 FROM ans")

question = "sum v1 mean v3 by id3" # q3
duckdb_bench("SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM x GROUP BY id3", "SELECT SUM(v1) AS v1, SUM(v3) AS v3 FROM ans")

question = "mean v1:v3 by id4" # q4
duckdb_bench("SELECT id4, AVG(v1) as v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM x GROUP BY id4", "SELECT SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM ans")

question = "sum v1:v3 by id6" # q5
duckdb_bench("select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6", "select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from ans")

question = "median v3 sd v3 by id4 id5" # q6
duckdb_bench("select id4, id5, median(v3) as median_v3, stddev(v3) as sd_v3 from x group by id4, id5", "select sum(median_v3) as median_v3, sum(sd_v3) as sd_v3 from ans")

question = "max v1 - min v2 by id3" # q7
duckdb_bench("select id3, max(v1)-min(v2) as range_v1_v2 from x group by id3", "select sum(range_v1_v2) as range_v1_v2 from ans")

question = "largest two v3 by id6" # q8
duckdb_bench("select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from x where v3 is not null) sub_query where order_v3 <= 2", "select sum(largest2_v3) as largest2_v3 from ans")

question = "regression v1 v2 by id2 id4" # q9
duckdb_bench("select id2, id4, pow(corr(v1, v2), 2) as r2 from x group by id2, id4", "select sum(r2) as r2 from ans")

question = "sum v3 count by id1:id6" # q10
duckdb_bench("select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6", "select sum(v3) as v3, sum(count) as count from ans")

cat(sprintf("grouping finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
