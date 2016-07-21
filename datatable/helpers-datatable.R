memory_usage = function() {
  as.numeric(system(paste("ps -o rss", Sys.getpid(), "| tail -1"), intern=TRUE)) / (1024^2)
}
