source("./_report/report.R")

download.time = function(file=c("logs.csv","time.csv"), from="https://h2oai.github.io/db-benchmark") {
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

drop.data.table = function(x, cols) {
  ans = data.table:::shallow(x)
  un = sapply(cols, function(col) uniqueN(x[[col]]))
  rm = names(un)[un <= 1L]
  if (length(rm)) set(ans, NULL, rm, NULL) # Rdatatable/data.table#4086
  ans
}

tail.time = function(solution, task, n=2L, i=seq_len(n), drop=TRUE) {
  stopifnot(length(solution)==1L, length(task)==1L, length(n)==1L, n>0L, length(i)>=1L, all(i>=0L))
  if (!missing(n) && !missing(i)) stop("only 'n' or 'i' argument should be used, not both")
  ld = time_logs()
  s = solution
  t = task
  ld = ld[solution==s & task==t]
  ub = unique(ld$batch)
  i = i[i <= length(ub)] # there might be only N unq batches but N+1 requested
  if (!length(i)) stop("there are not enough registered runs for this solution and requested recent timings")
  b = rev(ub)[i]
  ans = dcast(
    ld[batch%in%b],
    in_rows + knasorted + question_group + question ~ paste(format(as.POSIXct(as.numeric(batch), origin="1970-01-01"), "%Y%m%d"), substr(git, 1, 7), sep="_"),
    value.var = "time_sec_1"
  )
  if (drop) ans = drop.data.table(ans, cols=c("in_rows","knasorted","question_group","question"))
  ans
}

compare.time = function(solutions, task, drop=TRUE) {
  stopifnot(length(solutions)>=1L, length(task)==1L)
  ld = time_logs()
  t = task
  ans = dcast(
    ld[script_recent==TRUE & solution%in%solutions & task==t],
    in_rows + knasorted + question_group + question ~ solution,
    value.var = "time_sec_1"
  )
  if (drop) ans = drop.data.table(ans, cols=c("in_rows","knasorted","question_group","question"))
  ans
}

## maintainer mode
#scp -C mr-dl11:~/git/db-benchmark/logs.csv ~/git/db-benchmark/logs.csv && scp -C mr-dl11:~/git/db-benchmark/time.csv ~/git/db-benchmark/time.csv

## user mode
#download.time()
#tail.time("juliadf", "groupby", i=c(1L, 2L))
#tail.time("data.table", "groupby", i=c(1L, 2L))
#compare.time(c("data.table","spark","pydatatable"), "join")
