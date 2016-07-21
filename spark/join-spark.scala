
println("# join-spark.scala")

import org.apache.spark.sql.SQLContext
import org.joda.time._

:load ./helpers.scala

val sqlContext = SQLContext.getOrCreate(sc)
val ver = sc.version

/** load data */

val src_x = sys.env("SRC_X")
val src_y = sys.env("SRC_Y")
val t_start = DateTime.now()
val X = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(src_x)
val Y = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(src_y)
new Duration(t_start, DateTime.now())

val t_start = DateTime.now()
X.cache
Y.cache
new Duration(t_start, DateTime.now())

val t_start = DateTime.now() 
val in_rows = X.count
Y.count
new Duration(t_start, DateTime.now())

/** join */

System.gc()
val ans = X.join(Y, "KEY")
val t_start = DateTime.now()
ans.cache
val out_rows = ans.count
val t_end = DateTime.now()
val t = new Duration(t_start, t_end).getMillis.toDouble / 1000.toDouble
val m = Double.NaN
write_log(task="join", data="", in_rows=in_rows, out_rows=out_rows, solution="spark", version=ver, fun=".join", run=1:Int, time_sec=t, mem_gb=m)
ans.unpersist(blocking=true)

System.gc()
val ans = X.join(Y, "KEY")
val t_start = DateTime.now()
ans.cache 
val out_rows = ans.count
val t_end = DateTime.now()
val t = new Duration(t_start, t_end).getMillis.toDouble / 1000.toDouble
val m = Double.NaN
write_log(task="join", data="", in_rows=in_rows, out_rows=out_rows, solution="spark", version=ver, fun=".join", run=2:Int, time_sec=t, mem_gb=m)
ans.unpersist(blocking=true)

System.gc()
val ans = X.join(Y, "KEY")
val t_start = DateTime.now()
ans.cache
val out_rows = ans.count
val t_end = DateTime.now()
val t = new Duration(t_start, t_end).getMillis.toDouble / 1000.toDouble
val m = Double.NaN
write_log(task="join", data="", in_rows=in_rows, out_rows=out_rows, solution="spark", version=ver, fun=".join", run=3:Int, time_sec=t, mem_gb=m)
ans.unpersist(blocking=true)

/** cleanup and exit */

X.unpersist(blocking=true)
Y.unpersist(blocking=true)

sys.exit(0)
