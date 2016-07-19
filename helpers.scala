import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths

def write_log(task:String, data:String, in_rows:Long, out_rows:Long, solution:String, fun:String, run:Int, time_sec:Double, mem_gb:Double) : Boolean = {
    val some_batch = sys.env.get("BATCH")
    val batch = some_batch.getOrElse("")
    val timestamp = System.currentTimeMillis.toDouble / 1000.toDouble
    val comment = "" /** placeholder for updated to timing data */
    val some_log_file = sys.env.get("CSV_TIME_FILE")
    val log_file = some_log_file.getOrElse("time.csv")
    val log_file_exists = Files.exists(Paths.get(log_file.toString))
    if(time_sec.isNaN){
      var time_sec = ""
    }
    if(mem_gb.isNaN){
      var mem_gb = ""
    }
    val fw = new FileWriter(log_file.toString, true)
    val log_row = List(batch, "%.3f" format timestamp, task, data, in_rows, out_rows, solution, fun, run, time_sec, mem_gb, comment).mkString(",") + "\n"
    print("# " + log_row)
    if(!log_file_exists) {
      val header_row = List("batch","timestamp","task","data","in_rows","out_rows","solution","fun","run","time_sec","mem_gb","comment").mkString(",") + "\n"
      fw.write(header_row)
    }
    fw.write(log_row)
    fw.close()
    return true
}
