
println("# join-spark.scala")

import org.apache.spark.sql.SQLContext
import org.joda.time._
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths
val some_log_file = sys.env.get("CSV_TIME_FILE")
val log_file = some_log_file.getOrElse("time.csv")
val sqlContext = SQLContext.getOrCreate(sc)
def write_log( timestamp:Double, task:String, data:String, in_rows:Long, out_rows:Long, solution:String, fun:String, run:Int, time_sec:Double, mem_gb:Double, log_file:String) : Boolean = {
    val log_file_exists = Files.exists(Paths.get(log_file.toString))
    val fw = new FileWriter(log_file.toString, true)
    val log_row = List("%.5f" format timestamp, task, data, in_rows, out_rows, solution, fun, run, "%.3f" format time_sec, "%.3f" format mem_gb).mkString(",") + "\n"
    print("# " + log_row)
    if(!log_file_exists) {
      val header_row = List("timestamp","task","data","in_rows","out_rows","solution","fun","run","time_sec","mem_gb").mkString(",") + "\n"
      fw.write(header_row)
    }
    fw.write(log_row)
    fw.close()
    return true
}

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
write_log(timestamp=System.currentTimeMillis/1000, task="join", data="", in_rows=in_rows, out_rows=out_rows, solution="spark", fun=".join", run=1:Int, time_sec=t, mem_gb=m, log_file=log_file.toString)
ans.unpersist(blocking=true)

System.gc()
val ans = X.join(Y, "KEY")
val t_start = DateTime.now()
ans.cache 
val out_rows = ans.count
val t_end = DateTime.now()
val t = new Duration(t_start, t_end).getMillis.toDouble / 1000.toDouble
val m = Double.NaN
write_log(timestamp=System.currentTimeMillis/1000, task="join", data="", in_rows=in_rows, out_rows=out_rows, solution="spark", fun=".join", run=2:Int, time_sec=t, mem_gb=m, log_file=log_file.toString)
ans.unpersist(blocking=true)

System.gc()
val ans = X.join(Y, "KEY")
val t_start = DateTime.now()
ans.cache
val out_rows = ans.count
val t_end = DateTime.now()
val t = new Duration(t_start, t_end).getMillis.toDouble / 1000.toDouble
val m = Double.NaN
write_log(timestamp=System.currentTimeMillis/1000, task="join", data="", in_rows=in_rows, out_rows=out_rows, solution="spark", fun=".join", run=3:Int, time_sec=t, mem_gb=m, log_file=log_file.toString)
ans.unpersist(blocking=true)

/** cleanup and exit */

X.unpersist(blocking=true)
Y.unpersist(blocking=true)

sys.exit(0)
