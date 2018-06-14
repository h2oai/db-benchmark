## Nice bar plot of grouping benchmark timings based on Matt Dowle scripts from 2014
## https://github.com/h2oai/db-benchmark/commit/fce1b8c9177afb49471fcf483a438f619f1a992b
## Original grouping benchmark can be found in: https://github.com/Rdatatable/data.table/wiki/Benchmarks-:-Grouping

tests = "
# GROUPING
DT[, .(v1=sum(v1)), keyby=id1]
DF %>% group_by(id1) %>% summarise(sum(v1))
DF.groupby(['id1']).agg({'v1':'sum'})
DT[:, {'v1': sum(f.v1)}, f.id1]

DT[, .(v1=sum(v1)), keyby=.(id1, id2)]
DF %>% group_by(id1,id2) %>% summarise(sum(v1))
DF.groupby(['id1','id2']).agg({'v1':'sum'})
DT[:, {'v1': sum(f.v1)}, [f.id1, f.id2]] 

DT[, .(v1=sum(v1), v3=mean(v3)), keyby=id3]
DF %>% group_by(id3) %>% summarise(sum(v1),mean(v3))
DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})
DT[:, {'v1': sum(f.v1), 'v3': mean(f.v3)}, f.id3]

DT[, lapply(.SD, mean), keyby=id4, .SDcols=v1:v3]
DF %>% group_by(id4) %>% summarise_each(funs(mean), vars=7:9)
DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
DT[:, {'v1': mean(f.v1), 'v2': mean(f.v2), 'v3': mean(f.v3)}, f.id4]

DT[, lapply(.SD, sum), keyby=id6, .SDcols=v1:v3]
DF %>% group_by(id6) %>% summarise_each(funs(sum), vars=7:9)
DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})
DT[:, {'v1': sum(f.v1), 'v2': sum(f.v2), 'v3': sum(f.v3)}, f.id6]
"

source("helpers.R") # for solution date from gh repos
stopifnot(sapply(c("curl","jsonlite"), requireNamespace, quietly=TRUE)) # used for lookup date based on git
library(data.table)
if (!capabilities()[["X11"]] && capabilities()[["cairo"]]) options(bitmapType="cairo") # fix for R compiled with-x=no with-cairo=yes
benchplot = function(.nrow=Inf) {
  
  res = fread("time.csv")[batch==max(batch)][task=="groupby"][, task:=NULL]
  if (!is.finite(.nrow)) {
    .nrow = res[, max(in_rows)]
  }
  res = res[, .SD, .SDcols=c("time_sec","question","solution","in_rows","out_rows","out_cols","run","version","git")]
  res[, test := setNames(1:5, unique(res$question))[question]]
  setnames(res, c("time_sec","question","solution","in_rows","out_rows","out_cols"), c("elapsed","task","pkg","nrow","ansnrow","ansncol"))
  gb = fread("data.csv")[rows==.nrow & task=="groupby", gb[1L]] # [1L] will take k=1e2
  stopifnot(length(gb)==1L)
  res[, gb:=gb]
  res[git=="", git:=NA_character_]

  # h2oai/datatable#1082 grouping test2 currently (0,0) dummy frame
  res[pkg=="pydatatable" & task=="sum v1 by id1:id2", elapsed:=NA_real_]
  fix_missing = res[pkg=="data.table" & task=="sum v1 by id1:id2", .(ansnrow, ansncol)]
  res[pkg=="pydatatable" & task=="sum v1 by id1:id2", c("ansnrow","ansncol") := fix_missing]
  # pandas 1e9 killed on 120GB machine due to not enough memory
  if (.nrow >= 1e9 && res[pkg=="pandas", .N] < 15L) {
    fix_pandas = res[pkg=="data.table"][, elapsed:=NA_real_][, pkg:="pandas"][, gb:=NA_real_][, version:=NA_character_][,git:=NA_character_]
    res = rbindlist(list(res[pkg!="pandas"], fix_pandas))[order(pkg)]
  }
  
  fnam = paste0("grouping.",gsub("e[+]0","E",.nrow),".png")
  cat("Plotting to",fnam,"...\n")
  png(file = fnam, width=800, height=1000)
  
  par(mar=c(1.1,1.1,6.1,2.1)) # shift to the left
  
  ans = res[nrow==.nrow & run==1][order(test,pkg,decreasing=TRUE)]
  
  NPKG = 4L # data.table, dplyr, pandas, pydatatable
  pad = as.vector(sapply(0:4, function(x) c( as.vector(rbind(x*NPKG + 1:NPKG, NA)), NA, NA)))

  code = unlist(strsplit(tests, split="\n"))[-(1:2)]
  code = code[code!=""]
  
  # horiz=TRUE does it first bar from the bottom
  comma = function(x) format(as.integer(signif(x,4)),big.mark=",")
  lr = "#FF7777"
  lb = "#7777FF"
  lg = "#77FF77"; green = "green4"
  pydtcol = "orange"
  lpydtcol = "orange2"
  m = ans[,max(elapsed,na.rm=TRUE)]
  if (m>2*60*60) {
    timescale=3600; xlab="Hours"
  } else if (m>120) {
    timescale=60; xlab="Minutes"
  } else {
    timescale=1; xlab="Seconds"
  }
  bars = ans[,(elapsed)/timescale]
  at = pretty(bars,10)
  at = at[at!=0]
  tt = barplot(bars[pad], horiz=TRUE, xlim=c(0,tail(at,1)), axes=FALSE)
  tt = rev(tt)
  w = (tt[1]-tt[2])/4
  
  h1 = tt[1]
  abline(h=h1)
  ff = if (length(at)<=8) TRUE else -1  # ff = first first xaxis label overlap
  text(x=at[ff], y=h1, labels=format(at[ff]), adj=c(0.5,-0.5), cex=1.5, font=2, xpd=NA)
  text(x=0, y=h1, labels=xlab, adj=c(0,-0.5), font=2, cex=1.5, xpd=NA)
  
  h2 = tail(tt,1)-4*w
  abline(h=h2)
  text(x=at[ff], y=h2, labels=format(at[ff]), adj=c(0.5,+1.5), cex=1.5, font=2, xpd=NA)
  text(x=0, y=h2, labels=xlab, adj=c(0,+1.5), font=2, cex=1.5, xpd=NA)
  
  space = NPKG*2 + 2
  abline(h=tt[seq(space+1,by=space,length=4)], col="grey",lwd=2)
  for (x in at) lines(x=c(x,x), y=c(h1,h2), col="lightgrey", lwd=2, lty="dotted")
  barplot(bars[pad], horiz=TRUE, axes=FALSE,
          col=rep(c(pydtcol,green,"red","blue","black"),each=2), font=2, xpd=NA, add=TRUE)
  
  textBG = function(x,y,txt,...) {
    txtw = strwidth(txt, ...); txth = strheight(txt, ...);
    txty = y-2*w;  # w from calling scope above
    rect(x, txty, x+txtw, txty+1.8*txth, col="white", border=NA)
    text(x, y, txt, adj=c(0,0.7), ...)
  }
  
  for (i in 0:4) {
    textBG(0, tt[3+i*space], code[1+i*NPKG], col="blue", font=2)
    textBG(0, tt[5+i*space], code[2+i*NPKG], col="red", font=2)
    textBG(0, tt[7+i*space], code[3+i*NPKG], col=green, font=2)
    textBG(0, tt[9+i*space], code[4+i*NPKG], col=pydtcol, font=2)
    ansnrow = ans[test==i+1 & run==1,ansnrow]
    ansncol = ans[test==i+1 & run==1,ansncol]
    if (length(unique(ansnrow)) != 1) stop("ansnrow mismatch")
    #if (length(unique(ansncol)) != 1) stop("ansncol mismatch") # pd.ans.shape[1] does not return the actual columns and ans is pivot like
    ansnrow = ansnrow[1]
    ansncol = ansncol[1]
    textBG(0, tt[2+i*space], font=2, paste("Test", i+1, ":",
      comma(ansnrow),"ad hoc groups of",comma(ans[1,nrow]/ansnrow),"rows;  result",
      comma(ansnrow),"x",ansncol))
  }

  ans2 = res[nrow==.nrow & run==2][order(test,pkg)]
  for (i in 0:4) {
    at=tt[4+i*space]; rect(0, at-w, ans2[1+i*NPKG, (elapsed)/timescale], at+w, col=lb, xpd=NA)
    at=tt[6+i*space]; rect(0, at-w, ans2[2+i*NPKG, (elapsed)/timescale], at+w, col=lr, xpd=NA)
    at=tt[8+i*space]; rect(0, at-w, ans2[3+i*NPKG, (elapsed)/timescale], at+w, col=lg, xpd=NA)
    at=tt[10+i*space]; rect(0, at-w, ans2[4+i*NPKG, (elapsed)/timescale], at+w, col=lpydtcol, xpd=NA)
    if (is.na(ans2[2+i*NPKG, elapsed])) textBG(0, tt[6+i*space], "corrupt grouped_df: tidyverse/dplyr#3640", col="red", font=2)
    if (is.na(ans2[3+i*NPKG, elapsed])) textBG(0, tt[8+i*space], "MemoryError", col=green, font=2)
    if (is.na(ans2[4+i*NPKG, elapsed])) textBG(0, tt[10+i*space], "Not yet implemented", col=pydtcol, font=2)
  }
  cph = 0.30  # minimum on graph histories; what people will see if they check
  tn = res[nrow==.nrow, sum(elapsed, na.rm=TRUE), pkg]
  tn = setNames(tn$V1, tn$pkg)
  leg = unique(ans[order(pkg)], by="pkg"
               )[, tN := tn[pkg] # no joining to not reorder
                 ][, sprintf("%s %s %s  -  %s  -  Total: $%.02f for %s %s",
                             pkg, version, if (!is.na(git)) paste0("(",substr(git,1,7),")")  else "",
                             solution.date(pkg, version, git, only.date=TRUE, use.cache=TRUE),
                             cph*tN/3600, round(tN/timescale,0), tolower(xlab)), by=pkg
                   ][, V1]
  topoffset = 18.5
  legend(0,par()$usr[4]+topoffset*w, pch=22, pt.bg=c("blue","red",green,pydtcol), bty="n", cex=1.5, pt.cex=3.5,
         text.font=1, xpd=NA, legend=leg)
  mtext(paste("Input table:",comma(.nrow),"rows x 9 columns (",
        {gb<-ans[pkg=="data.table",gb[1]]; if (gb<1) round(gb,1) else 5*round(ceiling(gb)/5)},
        "GB ) - Random order",
        paste0("(as of ", format(as.POSIXct(ans[1L, batch], origin="1970-01-01")),")")), # add datetime of benchmark batch to plot title
        side=3, line=4.5, cex=1.5, adj=0, font=2)
  legend(par()$usr[2], par()$usr[4]+topoffset*w, pch=22, xpd=NA, xjust=1, bty="n", pt.lwd=1,
         legend=c("First time","Second time"), pt.cex=c(3.5,2.5), cex=1.5, pt.bg=c("blue",lb))
  dev.off()
  system(paste("/usr/bin/xdg-open",fnam), wait=FALSE) 
}
