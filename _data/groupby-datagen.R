# Rscript groupby-datagen.R 1e7 1e2 0 0 ## 1e7 rows, 1e2 K, 0% NAs, random order
# Rscript groupby-datagen.R 1e8 1e1 5 1 ## 1e8 rows, 10 K, 5% NAs, sorted order
args = commandArgs(TRUE)

pretty_sci = function(x) {
  tmp<-strsplit(as.character(x), "+", fixed=TRUE)[[1L]]
  if(length(tmp)==1L) {
    paste0(substr(tmp, 1L, 1L), "e", nchar(tmp)-1L)
  } else if(length(tmp)==2L){
    paste0(tmp[1L], as.character(as.integer(tmp[2L])))
  }
}

library(data.table)
N=as.integer(args[1L]); K=as.integer(args[2L]); nas=as.integer(args[3L]); sort=as.integer(args[4L])
stopifnot(nas<=100L, nas>=0L, sort%in%c(0L,1L))
set.seed(108)
cat(sprintf("Producing data of %s rows and %s K groups factors\n", pretty_sci(N), pretty_sci(K)))
DT = data.table(
  id1 = sample(sprintf("id%03d",1:K), N, TRUE),      # large groups (char)
  id2 = sample(sprintf("id%03d",1:K), N, TRUE),      # large groups (char)
  id3 = sample(sprintf("id%010d",1:(N/K)), N, TRUE), # small groups (char)
  id4 = sample(K, N, TRUE),                          # large groups (int)
  id5 = sample(K, N, TRUE),                          # large groups (int)
  id6 = sample(N/K, N, TRUE),                        # small groups (int)
  v1 =  sample(5, N, TRUE),                          # int in range [1,5]
  v2 =  sample(15, N, TRUE),                         # int in range [1,15]
  v3 =  round(runif(N,max=100),6)                    # numeric e.g. 23.574912
)
if (nas>0L) {
  real_nas = nas/100
  cat(sprintf("Turning %s of data in each column to NAs\n", real_nas))
  N_nas = as.integer(N*real_nas)
  for (col in names(DT)) {
    I_nas = sample(N, N_nas, replace=FALSE)
    set(DT, I_nas, col, NA)
  }
}
if (sort==1L) {
  cat(sprintf("Sorting data\n"))
  setkeyv(DT, paste0("id", 1:6))
}
file = sprintf("G1_%s_%s_%s_%s.csv", pretty_sci(N), pretty_sci(K), nas, sort)
cat(sprintf("Writing data to %s\n", file))
fwrite(DT, file)
cat(sprintf("Data written to %s, quitting\n", file))
if (!interactive()) quit("no", status=0)
