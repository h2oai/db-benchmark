
println("# join-spark.scala")

import org.apache.spark.sql.SQLContext
import org.joda.time._

:load ./helpers.scala

val sqlContext = SQLContext.getOrCreate(sc)
val task = "join"
val ver = sc.version
val question = "inner join"
val solution = "spark"
val fun = ".join"
val cache = "TRUE"

/** load data */

val src_x = sys.env("SRC_X")
val src_y = sys.env("SRC_Y")
val t_start = DateTime.now()
val X = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(src_x)
val Y = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(src_y)
new Duration(t_start, DateTime.now())

val data_name = get_data_name(List(src_x, src_y)) /** paste(basename(.) collapse="-") */

val t_start = DateTime.now()
X.cache()
Y.cache()
new Duration(t_start, DateTime.now())

val t_start = DateTime.now() 
val in_rows = X.count()
println(in_rows)
println(Y.count())
new Duration(t_start, DateTime.now())

/** join */

System.gc()
val t_start = DateTime.now()
val ans = X.join(Y, "KEY")
ans.cache()
println(ans.count())
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("X2"), sum("Y2")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)

System.gc()
val t_start = DateTime.now()
val ans = X.join(Y, "KEY")
ans.cache()
println(ans.count())
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("X2"), sum("Y2")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)

System.gc()
val t_start = DateTime.now()
val ans = X.join(Y, "KEY")
ans.cache()
println(ans.count())
val t = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
val out_rows = ans.count()
val m = Double.NaN
val t_start = DateTime.now()
val chk = ans.agg(sum("X2"), sum("Y2")).collect()
val chkt = new Duration(t_start, DateTime.now()).getMillis.toDouble / 1000.toDouble
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3:Int, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist(blocking=true)

/** cleanup and exit */

X.unpersist(blocking=true)
Y.unpersist(blocking=true)

sys.exit(0)
