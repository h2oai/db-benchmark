import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths

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
