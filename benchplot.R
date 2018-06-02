

# Must be in directory where res* files are

# setwd("~/R/Benchmark_Scripts")
# OUTPATH = "~/R/gitdatatable/data.table.wiki.git/bench/" # release

benchplot = function(.nrow=1e7) {
  require(data.table)
  if (!exists("OUTPATH")) OUTPATH = paste0(getwd(),"/")  # dev
  fnam = paste0(OUTPATH,"grouping.",gsub("e[+]0","E",.nrow),".png")
  cat("Plotting to",fnam,"...\n")
  png(file = fnam, width=800, height=1000)
  
  par(mar=c(1.1,1.1,6.1,2.1)) # shift to the left
  
  res = rbindlist( lapply(dir(patt="*.csv"), fread) )
  
  if (res[, any(abs(((user+sys) / elapsed)-1)>0.01 & user>0.0) ]) stop("user+sys != elapsed +/- 1%")
  # Python's timeit doesn't give us user and sys separately it seems
  
  res[,c("user","sys","user.child","sys.child"):=NULL]
  res = res[test>0]

  # sometimes the first row is read others not when there is no header.
  # *** TO DO: fread bug, TO DO

  ans = res[nrow==.nrow & run==1][base::order(test,pkg,decreasing=TRUE)]
                     # decreasing=TRUE for when we work with v1.9.2 on CRAN
  
  NPKG = 3L # data.table, dplyr and pandas
  pad = as.vector(sapply(0:4, function(x) c( as.vector(rbind(x*NPKG + 1:NPKG, NA)), NA, NA)))

  # tests variable from other file source first
  code = unlist(strsplit(tests, split="\n"))[-(1:2)]
  code = code[code!=""]

  # horiz=TRUE does it first bar from the bottom
  comma = function(x) format(as.integer(signif(x,4)),big.mark=",")
  lr = "#FF7777"
  lb = "#7777FF"
  lg = "#77FF77"; green = "green4"
  m = ans[,max(elapsed,na.rm=TRUE)]
  if (m>2*60*60) { timescale=3600; xlab="Hours" }
  else if (m>120) { timescale=60; xlab="Minutes" }
  else { timescale=1; xlab="Seconds" }
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
  # mgp= (see par) brings tick labels closer to axis, but we don't use axis() here.
  
  space = NPKG*2 + 2
  abline(h=tt[seq(space+1,by=space,length=4)], col="grey",lwd=2)
  for (x in at) lines(x=c(x,x), y=c(h1,h2), col="lightgrey", lwd=2, lty="dotted")
  barplot(bars[pad], horiz=TRUE, axes=FALSE, # xlim=c(0,tail(at,1)),
          col=rep(c(green,"red","blue","black"),each=2), font=2, xpd=NA, add=TRUE)
  
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
    ansnrow = ans[test==i+1 & run==1,ansnrow]
    ansncol = ans[test==i+1 & run==1,ansncol]
    if (length(unique(ansnrow)) != 1) stop("ansnrow mismatch")
    if (length(unique(ansncol)) != 1) stop("ansncol mismatch")
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
    if (is.na(ans2[3+i*NPKG, elapsed])) textBG(0, tt[8+i*space], "MemoryError", col=green, font=2)
  }
  t1 = res[nrow==.nrow & pkg=="data.table", sum(elapsed)]
  t2 = res[nrow==.nrow & pkg=="dplyr", sum(elapsed)]
  t3 = res[nrow==.nrow & pkg=="pandas", sum(elapsed)]
  cph = 0.30  # minimum on graph histories; what people will see if they check
  legend(0,par()$usr[4]+14*w, pch=22, pt.bg=c("blue","red",green), bty="n", cex=1.5, pt.cex=3.5,
         text.font=1, xpd=NA,
         legend=c(paste0("data.table 1.9.2  -  CRAN 27 Feb 2014  -  Total: $",
                    sprintf("%.02f",cph*t1/3600)," for ",round(t1/timescale,0)," ",tolower(xlab)),
                  paste0("dplyr 0.2  -  CRAN 21 May 2014  -  Total: $",
                    sprintf("%.02f",cph*t2/3600)," for ",round(t2/timescale,0)," ",tolower(xlab)),
                  paste0("pandas 0.14.1  -  PyPI 11 Jul 2014  -  Total: $",
                    sprintf("%.02f",cph*t3/3600)," for ",round(t3/timescale,0)," ",tolower(xlab))))
  mtext(paste("Input table:",comma(.nrow),"rows x 9 columns (",
        {gb<-ans[pkg=="data.table",gb[1]]; if (gb<1) round(gb,1) else 5*round(ceiling(gb)/5)},"GB ) - Random order"),
        side=3, line=4.5, cex=1.5, adj=0, font=2)
  # note that pandas uses double the memory, but we use the smaller data.table size in the title

  legend(par()$usr[2], par()$usr[4]+14*w, pch=22, xpd=NA, xjust=1, bty="n", pt.lwd=1,  #adj=1, # , # , #
         legend=c("First time","Second time"), pt.cex=c(3.5,2.5), cex=1.5, pt.bg=c("blue",lb))
  dev.off()
  system(paste("/usr/bin/eom",fnam), wait=FALSE) 
}


