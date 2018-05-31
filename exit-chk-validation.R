suppressPackageStartupMessages({
  library(bit64)
  library(data.table)
})
source("helpers.R")

DT = read_timing("time.csv")
# recent timings, single cache=FALSE scenario where available
dt = last_timing(x=DT)

# uniqueness HAVING - detect lack of consistency in query output within single benchmark runs - only recent benchmark run
uniqueness = function(dt, const=c("out_rows","chk","batch"), const.by=c("task", "data", "in_rows", "question", "solution")) {
  l = lapply(const, function(col) call(">", as.name(paste0("unq_",col)), 1L))
  ii = Reduce(function(c1, c2) substitute(.c1 | .c2, list(.c1=c1, .c2=c2)), l)
  dt[,.SD
     ][, paste0("unq_",const) := lapply(.SD, uniqueN), const.by, .SDcols=const
       ][eval(ii)
         ]
}
uniqueness(dt, c("out_rows","chk","batch")) -> const.check

# detect lack of consistency in query output between benchmark runs (just 2 most recent runs)
cby=c("task", "data", "in_rows", "question", "solution", "cache", "fun", "run")
DT[order(timestamp), tail(.SD, 2L), by=cby
   ][!is.na(chk), uniqueness(.SD, c("out_rows","chk"))
     ] -> change.check

# detect lack of out_rows match in query output between solutions
uniqueness(dt, const = "out_rows", const.by = c("task", "data", "in_rows", "question")) -> count.check

# detect lack of chk approximate match in query output between solutions
chk.approx = function(dt, precision=4) {
  # we split processing as chk has various number of fields
  split(dt[!is.na(chk), .(chk), c("task","data","in_rows","question","solution")],
        by=c("task","data","in_rows","question")) -> ldt
  diff.chk = function(x) {
    vcols = paste0("V",seq_along(strsplit(x[1L,chk],";")[[1L]]))
    copy(x)[, c(vcols) := tstrsplit(chk, ";")
            ][, c(vcols) := lapply(.SD, type.convert), .SDcols=vcols
              ][, paste0("mean_",vcols) := lapply(.SD, mean), .SDcols=vcols
                ][, paste0("rel_",vcols) := eval(as.call(c(as.name("list"), lapply(vcols, function(col) substitute(round(abs(x-mean_x)/mean_x, precision), list(x=as.name(col), mean_x=as.name(paste0("mean_", col))))))))
                  ][, .(mean_rel_chk = mean(unlist(.SD))), c("task","data","in_rows","question","solution","chk"), .SDcols=paste0("rel_",vcols)
                    ]
  }
  rbindlist(lapply(ldt, diff.chk))
}
chk.approx(dt, 8)[mean_rel_chk > 0] -> chk.check

# send data quality report
submit = function(x) {
  lf <- file.path(getwd(), "validation.csv")
  write.table(x[, reporter_batch_id := as.integer(Sys.getenv("BATCH", NA))][, reported_datetime := as.POSIXct(reporter_batch_id, origin="1970-01-01")], # current workflow
              file=lf,
              row.names = FALSE,
              col.names = !file.exists(lf),
              append = file.exists(lf),
              sep = ",")
}
if (nrow(report <- rbindlist(c(
  list(const = const.check),
  list(change = change.check),
  list(count = count.check),
  list(chk = chk.check) # not yet approximate
), use.names = TRUE, fill = TRUE, idcol = "check"))) submit(report)
