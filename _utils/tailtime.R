source("./_report/report.R")

download.dbb = function(file=c("logs.csv","time.csv"), from="https://h2oai.github.io/db-benchmark") {
  stopifnot(is.character(file), is.character(from), length(file)>=1L, length(from)==1L, !is.na(file), !is.na(from))
  if (all(file.exists(file))) {
    md5file = paste(file, "md5", sep=".")
    download.file(file.path(from, md5file), destfile=md5file)
    upstream = sapply(strsplit(sapply(setNames(md5file, file), readLines), split=" ", fixed=TRUE), `[[`, 1L)
    current = tools::md5sum(file)
    new = current[names(upstream)] != upstream
    file = names(new)[new]
    if (!length(file)) {
      cat("nothing to download, md5sum of local files match the upstream md5sum\n")
      return(invisible(NULL))
    }
  }
  download.file(file.path(from, file), destfile=file)
  return(invisible(NULL))
}

tailtime = function(solution, task, n=2L) {
  ld = time_logs()
  s = solution
  t = task
  ld = ld[solution==s & task==t]
  nbatch = tail(unique(ld$batch), n=n)
  dcast(
    ld[batch%in%nbatch],
    in_rows + knasorted + question_group + question ~ paste(format(as.POSIXct(as.numeric(batch), origin="1970-01-01"), "%Y%m%d"), substr(git, 1, 7), sep="_"),
    value.var="time_sec_1"
  )
}

download.dbb()
tailtime("data.table", "groupby")
