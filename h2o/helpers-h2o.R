memory_usage = function() {
  res <- h2o:::.h2o.fromJSON(jsonlite::fromJSON(h2o:::.h2o.doSafeGET(urlSuffix = h2o:::.h2o.__CLOUD), simplifyDataFrame = FALSE))
  sum(sapply(res$nodes, function(x) x$max_mem - x$free_mem) / (1024^3))
  NA_real_ # not reliable yet
}
h2o.git = function(ip=Sys.getenv("H2O_HOST","localhost"), port=as.integer(Sys.getenv("H2O_PORT","54321"))) {
  stopifnot(requireNamespace("jsonlite"))
  js = jsonlite::fromJSON(sprintf("http://%s:%s/3/About", ip, port))
  toString(js$entries$value[which(js$entries$name=="Build git hash")])
}
