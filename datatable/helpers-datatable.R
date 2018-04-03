memory_usage = function() {
  as.numeric(system(paste("ps -o rss", Sys.getpid(), "| tail -1"), intern=TRUE)) / (1024^2)
}
datatable.git = function() {
  dcf = read.dcf(system.file("DESCRIPTION", package="data.table"), fields="Revision")
  toString(dcf[, "Revision"])
}
