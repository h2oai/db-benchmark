source("./_report/report.R")

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
tailtime("data.table", "groupby")
