# Rscript join-datagen.R 1e7 0 0 0 ## 1e7 rows, 0 ignored, 0% NAs, random order
# Rscript join-datagen.R 1e8 0 5 1 ## 1e8 rows, 0 ignored, 5% NAs, sorted order
args = commandArgs(TRUE)
datadir = "data"
if (!dir.exists(datadir)) stop(sprintf("directory '%s' does not exists", datadir))
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
cat(sprintf("Producing data of %s rows\n", pretty_sci(N)))
all_levels = sprintf("id%011.0f", 1:(2*N)) # up to 1e11-1 fixed char size
some_dups = function(N, ratio) {
  tmp = sample(N, N*(1-ratio))
  sample(c(tmp, sample(tmp, N*ratio)))
}
# create main table
DT = data.table(
  id1 = sample(all_levels[1:N], N),                  # factor unique continuous range
  id2 = sample(all_levels, N),                       # factor unique
  id3 = all_levels[some_dups(N, 0.1)],               # factor 0.1 dups
  id4 = sample(N*1.5, N),                            # int unique continuous range
  id5 = sample(N*2, N),                              # int unique
  id6 = some_dups(N, 0.1),                           # int 0.1 dups
  v1 =  sample(round(runif(100,max=100),4), N, TRUE) # numeric
)
data_name = sprintf("J1_%s_%s_%s_%s", pretty_sci(N), "NA", nas, sort)

y_N = setNames(c(N, N/1e3, N/1e6), c("big","medium","small"))

cat(sprintf("Producing join tables of %s rows\n", paste(collapse=", ", sapply(y_N, pretty_sci))))
# create join tables
y_DT = lapply(y_N, function(n) DT[sample(n)]) # TODO recreate new, not sample
join_to_tbls = function(data_name) {
  x_n = as.numeric(strsplit(data_name, "_", fixed=TRUE)[[1L]][2L])
  y_n = setNames(c(x_n, x_n/1e3, x_n/1e6), c("big","medium","small"))
  sapply(sapply(y_n, pretty_sci), gsub, pattern="NA", x=data_name)
}
y_data_name = join_to_tbls(data_name)

if (nas>0L) {
  stop("not yet implemented")
  real_nas = nas/100
  cat(sprintf("Turning %s of data in each column to NAs\n", real_nas))
  N_nas = as.integer(N*real_nas)
  for (col in names(DT)) {
    I_nas = sample(N, N_nas, replace=FALSE)
    set(DT, I_nas, col, NA)
  }
}
if (sort==1L) {
  stop("not yet implemented")
  cat(sprintf("Sorting data\n"))
  setkeyv(DT, paste0("id", 1:6))
}
file = file.path(datadir, paste0(data_name, ".csv"))
cat(sprintf("Writing data to %s\n", file))
fwrite(DT, file)
y_file = file.path(datadir, paste0(y_data_name, ".csv"))
mapply(function(DT, file) fwrite(DT, file), y_DT, y_file) -> nul
cat(sprintf("Data written to %s, quitting\n", paste(y_file, collapse=", ")))
if (!interactive()) quit("no", status=0)
