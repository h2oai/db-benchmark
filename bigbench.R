
# We startup R, create the data and run each test twice. Reporting the time of both runs.

# setwd(tempdir())   # just for local testing

bigbench = function(N=1e6, parallel=N<=1e9, K=100L) {
  tests = unlist(strsplit(tests, split="\n"))[-1]
  ntests = length(tests)/3
  type = gsub("# ","",tests[1])  # e.g. grouping
  for (i in 1:2) {
    NTxt = gsub("e[+]0","E",N)  # shorter, easier to work in bash
    postfix = paste(i,type,NTxt,sep=".")
    fnam = paste0("script",postfix,".in")
    fnamResults = paste0("res",postfix,".csv")
    fnamCheck = paste0("check",postfix,".txt")
    pkg = c("data.table","dplyr")[i]
    cat("pkg,type,nrow,gb,test,run,ansnrow,ansncol,user,sys,elapsed,user.child,sys.child\n",file=fnamResults)
    cat(pkg,"; ",type,"; ",date(),"\n\n",sep="",file=fnamCheck)
    cat(paste0("N=",N,"; K=",K,"\n"),
        paste0("fnamResults ='",fnamResults,"'\n"),
        paste0("fnamCheck ='",fnamCheck,"'\n"),
        paste0("pkg ='",pkg,"'\n"),
        paste0("type ='",type,"'\n"),
        common,
        setup[i],
        columns,
        setup2,
        paste0("time( ",
               tests[0:(length(tests)-1)%%3 == i],
               " )",
               collapse="\n"),
        "\n\ncat('\\n\\nFINISHED', pkg, type, N, '\\n\\n')\n",  # to master console
        file=fnam, sep="")
    cat("Starting: ",fnam,"\n")
    system2("R", args="--vanilla", stdin=fnam, wait=!parallel)
  }
}

  common = c('
testNum = 0L
GB = 0  # size of test data (assigned later)
time = function(x, rep=2) {
  sub = substitute(x)
  for (i in 1:rep) {
    tt = system.time( ans <- eval(sub, envir=.GlobalEnv) )
    cat(pkg,type,N,GB,testNum,i,nrow(ans),ncol(ans),as.vector(tt), sep=",", file=fnamResults, append=TRUE)
    cat("\\n", file=fnamResults, append=TRUE)
    if (i==1 && testNum>0) {   # first time is for the data creation
      cat("nrow=",N,"; testNum=",testNum,": ",nrow(ans),"x",ncol(ans),
          "\\n==================\\n", sep="", file=fnamCheck, append=TRUE )
      write.table(head(ans), file=fnamCheck, sep=",", append=TRUE, quote=FALSE, col.names=FALSE, row.names=FALSE)
      cat("---\\n", file=fnamCheck, append=TRUE)
      write.table(tail(ans), file=fnamCheck, sep=",", append=TRUE, quote=FALSE, col.names=FALSE, row.names=FALSE)
      cat("\\n\\n", file=fnamCheck, append=TRUE)      
    }
    rm(ans)
  }
  testNum <<- testNum + 1L
}
  ')
  
  setup = c('
require(data.table)
set.seed(1)
time(rep=1, DT <- data.table(
  ','
require(dplyr)
set.seed(1)
time(rep=1, DF <- data.frame(stringsAsFactors=FALSE,
  ')

columns = '
  id1 = sample(sprintf("id%03d",1:K), N, TRUE),      # 100 char ids (small number of groups)
  id2 = sample(sprintf("id%03d",1:K), N, TRUE),      # 100 char ids (a second set)
  id3 = sample(sprintf("id%010d",1:(N/K)), N, TRUE), # n/100 char ids (large number of groups)
  id4 = sample(K, N, TRUE),                          # 100 integer ids
  id5 = sample(K, N, TRUE),                          # 100 integer ids (a second set)
  id6 = sample(N/K, N, TRUE),                        # n/100 integer ids
  v1 =  sample(5, N, TRUE),                          # int in range [1,5]
  v2 =  sample(5, N, TRUE),                          # int in range [1,5]
  v3 =  sample(round(runif(100,max=100),4), N, TRUE) # num e.g. 23.5749
))
'

  setup2 = '
GB = round(sum(gc()[,2])/1024, 3)
cat("\n\nGB =",GB,pkg,type,N,"\n\n\n",sep=" ")
  '
  
tests = "
# GROUPING
DT[, sum(v1), keyby=id1]
DF %>% group_by(id1) %>% summarise(sum(v1))
DF.groupby(['id1']).agg({'v1':'sum'})

DT[, sum(v1), keyby='id1,id2']
DF %>% group_by(id1,id2) %>% summarise(sum(v1))
DF.groupby(['id1','id2']).agg({'v1':'sum'})

DT[, list(sum(v1), mean(v3)), keyby=id3]
DF %>% group_by(id3) %>% summarise(sum(v1),mean(v3))
DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})

DT[, lapply(.SD, mean), keyby=id4, .SDcols=7:9]
DF %>% group_by(id4) %>% summarise_each(funs(mean), vars=7:9)
DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})

DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9]
DF %>% group_by(id6) %>% summarise_each(funs(sum), vars=7:9)
DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})
"


