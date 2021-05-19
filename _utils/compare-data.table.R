source("_utils/time.R")
if (system("tail -1 time.csv | cut -d',' -f2", intern=TRUE)!="1621364165")
  stop("time.csv and logs.csv should be as of 1621364165 batch run, filter out newer rows in those files")

## groupby ----

d = tail.time("data.table", "groupby", i=c(1L, 2L))
setnames(d, c("20210517_2f2f62d","20210518_2f2f62d"), c("th_40","th_20"))
if (nrow(d[(is.na(th_40) & !is.na(th_20)) | (!is.na(th_40) & is.na(th_20))])) {
  stop("number of threads had an impact on completion of queries")
} else {
  d = d[!is.na(th_40)]
}
d[, th_40_20:=th_40/th_20]

## improvement
d[, .(mean=mean(th_40_20), median=median(th_40_20)), .(in_rows)]
#   in_rows      mean    median
#1:     1e7 1.0242721 0.9609988
#2:     1e8 0.9378870 0.9455267
#3:     1e9 0.9506561 0.9569359
d[, .(mean=mean(th_40_20), median=median(th_40_20)), .(knasorted)]
#                                         knasorted      mean    median
#1:   1e2 cardinality factor, 0% NAs, unsorted data 1.0393667 0.9538973
#2:   1e1 cardinality factor, 0% NAs, unsorted data 0.9521915 0.9544223
#3:   2e0 cardinality factor, 0% NAs, unsorted data 0.9604950 0.9569359
#4: 1e2 cardinality factor, 0% NAs, pre-sorted data 0.9371154 0.9487804
#5:   1e2 cardinality factor, 5% NAs, unsorted data 0.9678192 0.9598999
d[, .(mean=mean(th_40_20), median=median(th_40_20)), .(question_group)]
#   question_group      mean    median
#1:          basic 0.9548596 0.9301310
#2:       advanced 0.9897345 0.9806791

## worst case by data
d[, .(mean=mean(th_40_20), median=median(th_40_20)), .(in_rows, knasorted)][which.max(mean)]
#   in_rows                                     knasorted     mean    median
#1:     1e7 1e2 cardinality factor, 0% NAs, unsorted data 1.239259 0.9620776
## best case by data
d[, .(mean=mean(th_40_20), median=median(th_40_20)), .(in_rows, knasorted)][which.min(mean)]
#   in_rows                                     knasorted      mean    median
#1:     1e8 1e2 cardinality factor, 0% NAs, unsorted data 0.9235102 0.9200373

## worst case for single question
d[which.max(th_40_20)]
#   in_rows                                     knasorted question_group          question th_40 th_20 th_40_20
#1:     1e7 1e2 cardinality factor, 0% NAs, unsorted data          basic sum v1 by id1:id2 0.413 0.118      3.5
## best case for single question
d[which.min(th_40_20)]
#   in_rows                                     knasorted question_group              question th_40  th_20  th_40_20
#1:     1e9 1e2 cardinality factor, 5% NAs, unsorted data          basic sum v1 mean v3 by id3 15.22 21.104 0.7211903

## join ----

d = tail.time("data.table", "join", i=c(1L, 2L))
setnames(d, c("20210517_2f2f62d","20210518_2f2f62d"), c("th_40","th_20"))
if (nrow(d[(is.na(th_40) & !is.na(th_20)) | (!is.na(th_40) & is.na(th_20))])) {
  stop("number of threads had an impact on completion of queries")
} else {
  d = d[!is.na(th_40)]
}
d[, th_40_20:=th_40/th_20]

## improvement
d[, .(mean=mean(th_40_20), median=median(th_40_20)), .(in_rows)]
#   in_rows      mean    median
#1:     1e7 1.0149302 1.0000000
#2:     1e8 0.9143243 0.9008573
d[, .(mean=mean(th_40_20), median=median(th_40_20)), .(knasorted)]
#                 knasorted      mean    median
#1:   0% NAs, unsorted data 0.9385902 0.9144130
#2:   5% NAs, unsorted data 0.9612286 0.9294773
#3: 0% NAs, pre-sorted data 0.9940629 0.9705720

## worst case by data
d[, .(mean=mean(th_40_20), median=median(th_40_20)), .(in_rows, knasorted)][which.max(mean)]
#   in_rows               knasorted     mean median
#1:     1e7 0% NAs, pre-sorted data 1.055906   1.05
## best case by data
d[, .(mean=mean(th_40_20), median=median(th_40_20)), .(in_rows, knasorted)][which.min(mean)]
#   in_rows             knasorted      mean    median
#1:     1e8 0% NAs, unsorted data 0.8983325 0.8773762

## worst case for single question
d[which.max(th_40_20)]
#   in_rows             knasorted               question th_40 th_20 th_40_20
#1:     1e7 5% NAs, unsorted data medium inner on factor 0.513 0.443 1.158014
## best case for single question
d[which.min(th_40_20)]
#   in_rows             knasorted            question th_40 th_20  th_40_20
#1:     1e8 0% NAs, unsorted data medium outer on int 8.143 9.558 0.8519565
