import java.io.File
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths

def write_log(task:String, data:String, in_rows:Long, question:String, out_rows:Long, solution:String, version:String, fun:String, run:Int, time_sec:Double, mem_gb:Double, cache:String, chk:String, chk_time_sec:Double) : Boolean = {
    val some_batch = sys.env.get("BATCH")
    val batch = some_batch.getOrElse("")
    val timestamp = System.currentTimeMillis.toDouble / 1000.toDouble
    val comment = "" /** placeholder for updates to timing data */
    val some_log_file = sys.env.get("CSV_TIME_FILE")
    val log_file = some_log_file.getOrElse("time.csv")
    val log_file_exists = Files.exists(Paths.get(log_file.toString))
    if(time_sec.isNaN){
      var time_sec = ""
    }
    if(mem_gb.isNaN){
      var mem_gb = ""
    }
    val git = ""
    val fw = new FileWriter(log_file.toString, true)
    val log_row = List(batch, "%.3f" format timestamp, task, data, in_rows, question, out_rows, solution, version, git, fun, run, time_sec, mem_gb, cache, chk, chk_time_sec, comment).mkString(",") + "\n"
    print("# " + log_row)
    if(!log_file_exists) {
      val header_row = List("batch","timestamp","task","data","in_rows","question","out_rows","solution","version","git","fun","run","time_sec","mem_gb", "cache","chk","chk_time_sec","comment").mkString(",") + "\n"
      fw.write(header_row)
    }
    fw.write(log_row)
    fw.close()
    return true
}

def get_data_name(x:List[String]) : String = {
   var nm = for(ix <- x) yield (new File(ix)).getName()
   return nm.mkString("-")
}

def make_check(values:Array[org.apache.spark.sql.Row]) : String = {
   return values(0).mkString(";").replace(",","_")
}
