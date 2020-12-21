timeleft = function() {
  l = data.table::fread("logs.csv")
  if (!nrow(l))
    stop("logs.csv files is empty")
  this = l[.N]
  if (this$action=="finish") {
    this[, cat(sprintf("%s %s %s must have just finished\n", solution, task, data))]
    quit("no")
  }
  stopifnot(this$action=="start")
  l = l[-.N][action!="skip", data.table::dcast(.SD, solution+task+data+batch~action, value.var="timestamp")]
  took = l[this, on=.(solution, task, data), nomatch=NULL, finish[.N]-start[.N]]
  if (is.na(took)) {
    this[, cat(sprintf("%s %s %s is running for the first time so it is unknown how much it will run\n", solution, task, data))]
    quit("no")
  }
  stopifnot(took>0)
  now = trunc(as.numeric(Sys.time()))
  this[, cat(sprintf("%s %s %s should take around %ss more\n", solution, task, data, trunc(took-(now-timestamp))))]
  q("no")
}
