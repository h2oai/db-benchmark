
println("# sort-spark.scala")

import org.apache.spark.sql.SQLContext
import org.joda.time._

:load ./helpers.scala

val sqlContext = SQLContext.getOrCreate(sc)
val task = "sort"
val ver = sc.version
val question = "by int KEY"
val solution = "spark"
val fun = ".sort"
val cache = "TRUE"

/** load data */

println("loading datasets...")
val src_x = sys.env("SRC_X")
val t_start = DateTime.now()
val X = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(src_x)
new Duration(t_start, DateTime.now())

val data_name = get_data_name(List(src_x)) /** paste(basename(.) collapse="-") */

val t_start = DateTime.now()
X.cache()
new Duration(t_start, DateTime.now())

val t_start = DateTime.now() 
val in_rows = X.count()
println(in_rows)
new Duration(t_start, DateTime.now())

/** sort */
println("sorting...")

System.gc()
val t_start = DateTime.now()
val ans = X.sort("KEY")
ans.cache()
println(ans.count())
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("X2")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)

System.gc()
val t_start = DateTime.now()
val ans = X.sort("KEY")
ans.cache()
println(ans.count())
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("X2")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)

System.gc()
val t_start = DateTime.now()
val ans = X.sort("KEY")
ans.cache()
println(ans.count())
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("X2")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)

/** cleanup and exit */

X.unpersist(blocking=true)

sys.exit(0)
