
println("# groupby-spark.scala")

import org.apache.spark.sql.SQLContext
import org.joda.time._

:load ./helpers.scala

val sqlContext = SQLContext.getOrCreate(sc)
val task = "groupby"
val ver = sc.version
val solution = "spark"
val cache = "TRUE"

/** load data */

val src_grp = sys.env("SRC_GRP")
val data_name = get_data_name(List(src_grp)) /** paste(basename(.) collapse="-") */
val t_start = DateTime.now()
val X = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(src_grp)
new Duration(t_start, DateTime.now())

val t_start = DateTime.now()
X.cache()
new Duration(t_start, DateTime.now())

val t_start = DateTime.now() 
val in_rows = X.count()
in_rows
new Duration(t_start, DateTime.now())

/** groupby */

val question = "sum v1 by id1" /** #1 */
val fun = ".groupBy sum"
System.gc()

val t_start = DateTime.now()
val ans = X.groupBy("id1").sum("v1")
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("sum(v1)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id1").sum("v1")
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("sum(v1)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id1").sum("v1")
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("sum(v1)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)

val question = "sum v1 by id1:id2" /** #2 */
val fun = ".groupBy sum"
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id1","id2").sum("v1")
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("sum(v1)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id1","id2").sum("v1")
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("sum(v1)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id1","id2").sum("v1")
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("sum(v1)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)

val question = "sum v1 mean v3 by id3" /** #3 */
val fun = ".groupBy agg sum mean"
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id3").agg(sum("v1"), mean("v3"))
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("sum(v1)"), sum("avg(v3)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id3").agg(sum("v1"), mean("v3"))
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("sum(v1)"), sum("avg(v3)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id3").agg(sum("v1"), mean("v3"))
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("sum(v1)"), sum("avg(v3)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)

val question = "mean v1:v3 by id4" /** #4 */
val fun = ".groupBy mean"
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id4").mean("v1","v2","v3")
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("avg(v1)"), sum("avg(v2)"), sum("avg(v3)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id4").mean("v1","v2","v3")
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("avg(v1)"), sum("avg(v2)"), sum("avg(v3)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id4").mean("v1","v2","v3")
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("avg(v1)"), sum("avg(v2)"), sum("avg(v3)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)

val question = "sum v1:v3 by id6" /** #5 */
val fun = ".groupBy sum"
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id6").sum("v1","v2","v3")
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("sum(v1)"), sum("sum(v2)"), sum("sum(v3)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id6").sum("v1","v2","v3")
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("sum(v1)"), sum("sum(v2)"), sum("sum(v3)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)
System.gc()
val t_start = DateTime.now()
val ans = X.groupBy("id6").sum("v1","v2","v3")
ans.cache()
ans.count()
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("sum(v1)"), sum("sum(v2)"), sum("sum(v3)")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_check(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)

/** cleanup and exit */

X.unpersist(blocking=true)

sys.exit(0)
