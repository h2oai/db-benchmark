#!/usr/bin/env Rscript

cat("# join-duckdb\n")

source("./_helpers/helpers.R")

suppressPackageStartupMessages({
  library("DBI", warn.conflicts=FALSE)
  library("duckdb", lib.loc="./duckdb", warn.conflicts=FALSE)
})
ver = packageVersion("duckdb")
git = ""
task = "join"
solution = "duckdb"
cache = TRUE
on_disk = FALSE

data_name = Sys.getenv("SRC_DATANAME")
src_jn_x = file.path("data", paste(data_name, "csv", sep="."))
y_data_name = join_to_tbls(data_name)
src_jn_y = setNames(file.path("data", paste(y_data_name, "csv", sep=".")), names(y_data_name))
stopifnot(length(src_jn_y)==3L)
cat(sprintf("loading datasets %s\n", paste(c(data_name, y_data_name), collapse=", ")))

con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
git = dbGetQuery(con, "select source_id from pragma_version()")[[1]]

duckdb_import <- function(table, file) invisible(dbExecute(con, sprintf("CREATE TABLE %s as SELECT * FROM READ_CSV_AUTO('%s')", table, file)))

duckdb_import("x", src_jn_x)
duckdb_import("small", src_jn_y[1L])
duckdb_import("medium", src_jn_y[2L])
duckdb_import("big", src_jn_y[3L])

duckdb_nrow <- function(table) dbGetQuery(con, sprintf("SELECT COUNT(*) c FROM %s", table))$c

print(duckdb_nrow("x"))
print(duckdb_nrow("small"))
print(duckdb_nrow("medium"))
print(duckdb_nrow("big"))

task_init = proc.time()[["elapsed"]]
cat("joining...\n")


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

question = "small inner on int" # q1
fun = "inner_join"
duckdb_bench("select x.*, small.id4 AS small_id4,v2 from x join small using (id1)", "select sum(v1) as v1, sum(v2) as v2 from ans")

question = "medium inner on int" # q2
fun = "inner_join"
duckdb_bench("select x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4,medium.id5 AS medium_id5,v2
 from x join medium using (id2)", "select sum(v1) as v1, sum(v2) as v2 from ans")

question = "medium outer on int" # q3
fun = "left_join"
duckdb_bench("select x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4,medium.id5 AS medium_id5,v2 from x left join medium using (id2)", "select sum(v1) as v1, sum(v2) as v2 from ans")

question = "medium inner on factor" # q4
fun = "inner_join"
duckdb_bench("select x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4,v2 from x join medium using (id5)", "select sum(v1) as v1, sum(v2) as v2 from ans")


question = "big inner on int" # q5
fun = "inner_join"
duckdb_bench("select x.*, big.id1 as big_id1,big.id2 as big_id2, big.id4 as big_id4, big.id5 as big_id5,big.id6 as big_id6,v2
 from x join big using (id3)", "select sum(v1) as v1, sum(v2) as v2 from ans")

cat(sprintf("joining finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
