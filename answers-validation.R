source("report.R")
d = time_logs()

# this script meant to detect some inconsistencies within a solution results and between solutions results
# note that known exceptions has been already filtered out in report.R in clean_time function

check = list()

# detect lack of consistency in query output within single benchmark runs within each solution separately
grain = c("solution","task","data","iquestion")
d[!is.na(out_rows), .(unqn_out_rows=uniqueN(out_rows), unq_out_rows=paste(unique(out_rows), collapse=",")), by=grain
  ][unqn_out_rows>1L
    ] -> check[["solution_out_rows"]]
d[!is.na(chk) & solution!="cudf", # cudf check disabled as of now, see comment in model_time() in report.R
  .(unqn_chk=uniqueN(chk), unq_chk=paste(unique(chk), collapse=",")), by=grain
  ][unqn_chk>1L
    ] -> check[["solution_chk"]]

# detect lack of out_rows match in query output between solutions
grain = c("task","data","iquestion","question")
d[!is.na(out_rows), .(unqn_out_rows=uniqueN(out_rows), unq_out_rows=paste(unique(out_rows), collapse=",")), by=grain
  ][unqn_out_rows>1L
    ] -> check[["out_rows"]]
# detect lack of chk match in query output between median chk from all solutions with tolerance=0.005
chk_check = function(chk, tolerance=sqrt(.Machine$double.eps)) {
  len = unique(sapply(chk, length))
  if (length(len)!=1L) stop("some solutions returns chk for less variables than others")
  med = sapply(seq.int(len), function(i) median(sapply(chk, `[[`, i)))
  eq_txt = sapply(chk, all.equal, med, tolerance=tolerance, simplify=FALSE)
  #if (any(!sapply(eq_txt, isTRUE))) browser()
  eq = sapply(eq_txt, isTRUE)
  ans = list()
  ans$n_match = sum(eq)
  ans$n_mismatch = sum(!eq)
  ans$med_chk = paste0(format(med, scientific=FALSE, trim=TRUE), collapse=";")
  ans$sol_mismatch = if (!ans$n_mismatch) NA_character_ else paste0(names(eq)[!eq], collapse=",")
  ans$sol_chk_mismatch = if (!ans$n_mismatch) NA_character_ else paste(paste0(names(eq)[!eq], ":", sapply(sapply(chk[names(eq)[!eq]], format, scientific=FALSE, trim=TRUE, simplify=FALSE), paste, collapse=";")), collapse=",")
  ans
}
(if (nrow(check[["solution_chk"]])) NULL else { # only proceed if chk was not mismatched within a solution
  d[!is.na(chk) & solution!="cudf", # cudf chk validation disabled due to issue described in model_time() in report.R
    .(unqn_chk=uniqueN(chk), chk=unique(chk)), by=c("solution", grain)
    ][, if (any(unqn_chk>1L)) stop("this check should not be performed, should be escaped in 'if' branch") else .SD # ensure chk is unique
      ][, .(chk, chk_l=sapply(strsplit(chk, ";", fixed=TRUE), as.numeric, simplify=FALSE)), by=c("solution", grain)
        ][, chk_check(setNames(chk_l, solution), tolerance=0.005), keyby=grain
          ][n_mismatch>0L]
}) -> check[["chk"]]

# detect solutions for which chk calculation timing was relatively big comparing to query timing
grain = c("solution","task","data","iquestion","question")
d[, .(time_sec_1, chk_time_sec_1, time_sec_2, chk_time_sec_2, time_to_chk_1=time_sec_1/chk_time_sec_1, time_to_chk_2=time_sec_2/chk_time_sec_2), by=grain
  ][!(time_to_chk_1>2.5 & time_to_chk_2>2.5) # spark chk is only 2.6+ times faster than query
    ] -> check[["chk_time_sec"]]

# print results
if (any(sapply(check, nrow))) {
  cat("db-benchmark answers consistency check failed, see details below\n")
  print(check)
} else {
  cat("db-benchmark answers consistency check successfully passed\n")
}
