## Nice bar plot of grouping benchmark timings based on Matt Dowle scripts from 2014
## https://github.com/h2oai/db-benchmark/commit/fce1b8c9177afb49471fcf483a438f619f1a992b
## Original grouping benchmark can be found in: https://github.com/Rdatatable/data.table/wiki/Benchmarks-:-Grouping

groupby.code = list(
  "sum v1 by id1" = c(
    "dask"="x.groupby(['id1']).agg({'v1':'sum'}).compute()",
    "data.table"="DT[, .(v1=sum(v1)), keyby=id1]",
    "dplyr"="DF %>% group_by(id1) %>% summarise(sum(v1))",
    "juliadf"="by(x, :id1) do df; DataFrame(v1 = sum(df.v1)); end;",
    "pandas"="DF.groupby(['id1']).agg({'v1':'sum'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1)}, f.id1]",
    "spark"="spark.sql('select sum(v1) as v1 from x group by id1')"
    ),
  "sum v1 by id1:id2" = c(
    "dask"="x.groupby(['id1','id2']).agg({'v1':'sum'}).compute()",
    "data.table"="DT[, .(v1=sum(v1)), keyby=.(id1, id2)]",
    "dplyr"="DF %>% group_by(id1,id2) %>% summarise(sum(v1))",
    "juliadf"="by(x, [:id1, :id2]) do df; DataFrame(v1 = sum(df.v1)); end;",
    "pandas"="DF.groupby(['id1','id2']).agg({'v1':'sum'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1)}, [f.id1, f.id2]]",
    "spark"="spark.sql('select sum(v1) as v1 from x group by id1, id2')"
    ),
  "sum v1 mean v3 by id3" = c(
    "dask"="x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'}).compute()",
    "data.table"="DT[, .(v1=sum(v1), v3=mean(v3)), keyby=id3]",
    "dplyr"="DF %>% group_by(id3) %>% summarise(sum(v1), mean(v3))",
    "juliadf"="by(x, :id3) do df; DataFrame(v1 = sum(df.v1), v3 = mean(df.v3)); end;",
    "pandas"="DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1), 'v3': mean(f.v3)}, f.id3]",
    "spark"="spark.sql('select sum(v1) as v1, mean(v3) as v3 from x group by id3')"
    ),
  "mean v1:v3 by id4" = c(
    "dask"="x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()",
    "data.table"="DT[, lapply(.SD, mean), keyby=id4, .SDcols=v1:v3]",
    "dplyr"="DF %>% group_by(id4) %>% summarise_each(funs(mean), vars=7:9)",
    "juliadf"="by(x, :id4) do df; DataFrame(v1 = mean(df.v1), v2 = mean(df.v2), v3 = mean(df.v3)); end;",
    "pandas"="DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})",
    "pydatatable"="DT[:, {'v1': mean(f.v1), 'v2': mean(f.v2), 'v3': mean(f.v3)}, f.id4]",
    "spark"="spark.sql('select mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from x group by id4')"
    ),
  "sum v1:v3 by id6" = c(
    "dask"="x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()",
    "data.table"="DT[, lapply(.SD, sum), keyby=id6, .SDcols=v1:v3]",
    "dplyr"="DF %>% group_by(id6) %>% summarise_each(funs(sum), vars=7:9)",
    "juliadf"="by(x, :id6) do df; DataFrame(v1 = sum(df.v1), v2 = sum(df.v2), v3 = sum(df.v3)); end;",
    "pandas"="DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1), 'v2': sum(f.v2), 'v3': sum(f.v3)}, f.id6]",
    "spark"="spark.sql('select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6')"
    )
)

source("helpers.R") # for solution date based on git sha from github repositories, if no git revision available then hardcoded dictionary
stopifnot(sapply(c("curl","jsonlite"), requireNamespace, quietly=TRUE)) # used for lookup date based on git
library(data.table)
if (!capabilities()[["X11"]] && capabilities()[["cairo"]]) options(bitmapType="cairo") # fix for R compiled with-x=no with-cairo=yes

benchplot = function(.nrow=Inf, task="groupby", timings, code) {
  
  if (missing(code)) stop("provide 'code' argument, list of questions and respective queries in each solution")
  if (uniqueN(timings$batch)!=1L) stop("all timings to be presented has to be produced from same benchmark batch, `uniqueN(timings$batch)` must be equal to 1, there should be no NAs in 'batch' field")
  vbatch = timings$batch[1L]
  timings.task = unique(timings$task)
  if (length(intersect(timings.task, task))!=1L) stop("there should be only single task to present on benchplot, provide 'task' argument which exists in 'timings' dataset")
  vtask = task
  timings = timings[task==vtask]
  if (!is.finite(.nrow)) .nrow = timings[, max(in_rows)]
  
  exceptions = TRUE
  if (exceptions) {
    pandas_version = timings[solution=="pandas" & in_rows==min(in_rows), version[1L]]
    pandas_git = timings[solution=="pandas" & in_rows==min(in_rows), git[1L]]
    dask_version = timings[solution=="dask" & in_rows==min(in_rows), version[1L]]
    dask_git = timings[solution=="dask" & in_rows==min(in_rows), git[1L]]
  }
  
  timings = timings[in_rows==.nrow]
  
  questions = unique(timings$question)
  nquestions = length(questions)
  runs = unique(timings$run)
  nruns = length(runs)
  data = unique(timings$data)
  ndata = length(data)
  if (ndata!=1L) stop("only single data supported in benchplot")
  
  #timings[,.N,solution]
  if (exceptions) {
    # h2oai/datatable#1082 grouping by multiple cols not yet implemented, reset time_sec tot NA, impute out_rows and out_cols
    timings[solution=="pydatatable" & question=="sum v1 by id1:id2", time_sec:=NA_real_]
    fix_missing = timings[solution=="data.table" & question=="sum v1 by id1:id2", .(out_rows, out_cols)]
    timings[solution=="pydatatable" & question=="sum v1 by id1:id2", c("out_rows","out_cols") := fix_missing]
    
    # pandas 1e9 killed on 125GB machine due to not enough memory
    if (timings[solution=="pandas" & in_rows==1e9, uniqueN(question)*uniqueN(run)] < nquestions*nruns) {
      pandasi = timings[solution=="pandas" & in_rows==1e9, which=TRUE] # there might be some results, so we need to filter them out
      fix_pandas = timings[solution=="data.table" & in_rows==1e9
                           ][, time_sec:=NA_real_
                             ][, solution:="pandas"
                               ][, version:=pandas_version
                                 ][, git:=pandas_git]
      timings = rbindlist(list(timings[!pandasi], fix_pandas))[order(solution)]
    }
    # dask 1e9 killed on 125GB machine due to not enough memory
    if (timings[solution=="dask" & in_rows==1e9, uniqueN(question)*uniqueN(run)] < nquestions*nruns) {
      daski = timings[solution=="dask" & in_rows==1e9, which=TRUE] # there might be some results, so we need to filter them out
      fix_dask = timings[solution=="data.table" & in_rows==1e9
                         ][, time_sec:=NA_real_
                           ][, solution:="dask"
                             ][, version:=dask_version
                               ][, git:=dask_git]
      timings = rbindlist(list(timings[!daski], fix_dask))[order(solution)]
    }
  }
  #timings[,.N,solution]
  
  solutions = unique(timings$solution)
  nsolutions = length(solutions)
  
  gb = NA_real_
  if (length(intersect(list.files(pattern="\\.csv$"), data))) gb = file.info(data)$size/1024^3
  
  # keep only required columns
  timings = timings[, .SD, .SDcols=c("time_sec","question","solution","in_rows","out_rows","out_cols","run","version","git","batch")]
  timings[git=="", git:=NA_character_]
  # add question order
  timings[as.data.table(list(question=questions))[, I:=.I][], nquestion := i.I, on="question"]

  fnam = paste0(task, ".", gsub("e[+]0", "E", pretty_sci(.nrow)), ".png")
  if (interactive()) cat("Plotting to",fnam,"...\n")
  png(file = fnam, width=800, height=1200)
  
  par(mar=c(0.6, 1.1, 8.1, 2.1)) # shift to the left
  
  ans1 = timings[run==1L][order(nquestion, solution, decreasing=TRUE)]
  
  pad = as.vector(sapply(0:4, function(x) c(as.vector(rbind(x*nsolutions + 1:nsolutions, NA)), NA, NA)))

  # horiz=TRUE does it first bar from the bottom
  comma = function(x) format(as.integer(signif(x,4)),big.mark=",")
  lr = "#FF7777"
  lb = "#7777FF"
  lg = "#77FF77"; green = "green4"
  pydtcol = "orange2"
  lpydtcol = "orange"
  sparkcol = "#8000FFFF"
  lsparkcol = "#CC66FF"
  colors = c(sparkcol, pydtcol, green, "red", "blue", "black")
  m = ans1[,max(time_sec,na.rm=TRUE)]
  if (m > 2*60*60) {
    timescale = 3600
    xlab = "Hours"
  } else if (m > 120) {
    timescale = 60
    xlab = "Minutes"
  } else {
    timescale = 1
    xlab = "Seconds"
  }
  bars = ans1[, time_sec/timescale]
  # this is used for text(pmax(bars, bars2)) only to not overlap text with 2nd timing bar
  bars2 = timings[run==2L][order(nquestion, solution, decreasing=TRUE)][, time_sec/timescale]
  at = pretty(bars, 10)
  at = at[at!=0]
  tt = barplot(bars[pad], horiz=TRUE, xlim=c(0, tail(at, 1)), axes=FALSE)
  max_t = pmax(bars, bars2) # we put timing value as max(run1, run2), otherwise bigger bar would be overlapping text
  text(max_t, tt[!is.na(pad)]-0.15, round(max_t, 1), pos=4, cex=1.25)
  tt = rev(tt)
  w = (tt[1]-tt[2])/4
  
  h1 = tt[1]
  abline(h=h1)
  ff = if (length(at)<=8) TRUE else -1  # ff = first first xaxis label overlap
  text(x=at[ff], y=h1, labels=format(at[ff]), adj=c(0.5, -0.5), cex=1.5, font=2, xpd=NA)
  text(x=0, y=h1, labels=xlab, adj=c(0, -0.5), font=2, cex=1.5, xpd=NA)
  
  h2 = tail(tt, 1)-4*w
  abline(h=h2)
  text(x=at[ff], y=h2, labels=format(at[ff]), adj=c(0.5, 1.5), cex=1.5, font=2, xpd=NA)
  text(x=0, y=h2, labels=xlab, adj=c(0, 1.5), font=2, cex=1.5, xpd=NA)
  
  space = nsolutions*2 + 2
  abline(h=tt[seq(space+1, by=space, length=4)], col="grey", lwd=2)
  for (x in at) lines(x=c(x, x), y=c(h1, h2), col="lightgrey", lwd=2, lty="dotted")
  barplot(bars[pad], horiz=TRUE, axes=FALSE,
          col=rep(colors, each=2), font=2, xpd=NA, add=TRUE)
  
  textBG = function(x, y, txt, ...) {
    txtw = strwidth(txt, ...); txth = strheight(txt, ...);
    txty = y-2*w;  # w from calling scope above
    rect(x, txty, x+txtw, txty+1.8*txth, col="white", border=NA)
    text(x, y, txt, adj=c(0, 0.7), ...)
  }
  
  for (i in 0:4) {
    q = questions[i+1]
    textBG(0, tt[3+i*space], code[[q]][["data.table"]], col="blue", font=2)
    textBG(0, tt[5+i*space], code[[q]][["dplyr"]], col="red", font=2)
    textBG(0, tt[7+i*space], code[[q]][["pandas"]], col=green, font=2)
    textBG(0, tt[9+i*space], code[[q]][["pydatatable"]], col=pydtcol, font=2)
    textBG(0, tt[11+i*space], code[[q]][["spark"]], col=sparkcol, font=2)
    out_rows = ans1[question==q & run==1L, out_rows]
    out_cols = ans1[question==q & run==1L, out_cols]
    if (length(unique(out_rows)) != 1) stop("out_rows mismatch")
    #if (length(unique(out_cols)) != 1) stop("out_cols mismatch") # pd.ans.shape[1] does not return the actual columns and ans is pivot like
    out_rows = out_rows[1]
    Mode = function(x) {tx<-table(x); as.numeric(names(tx)[which.max(tx)])}
    out_cols = Mode(out_cols) # pandas and spark does not return grouping column
    textBG(0, tt[2+i*space], font=2, paste("Question", i+1, ":",
      comma(out_rows), "ad hoc groups of", comma(ans1[1L, in_rows]/out_rows), "rows;  result",
      comma(out_rows), "x", out_cols))
  }

  ans2 = timings[run==2L][order(nquestion, solution)]
  for (i in 0:4) {
    at=tt[4+i*space]; rect(0, at-w, ans2[1+i*nsolutions, (time_sec)/timescale], at+w, col=lb, xpd=NA)
    at=tt[6+i*space]; rect(0, at-w, ans2[2+i*nsolutions, (time_sec)/timescale], at+w, col=lr, xpd=NA)
    at=tt[8+i*space]; rect(0, at-w, ans2[3+i*nsolutions, (time_sec)/timescale], at+w, col=lg, xpd=NA)
    at=tt[10+i*space]; rect(0, at-w, ans2[4+i*nsolutions, (time_sec)/timescale], at+w, col=lpydtcol, xpd=NA)
    at=tt[12+i*space]; rect(0, at-w, ans2[5+i*nsolutions, (time_sec)/timescale], at+w, col=lsparkcol, xpd=NA)
    # exceptions: pandas and pydatatable
    if (is.na(ans2[3+i*nsolutions, time_sec])) textBG(0, tt[8+i*space], "Lack of memory to read data", col=green, font=2)
    if (is.na(ans2[4+i*nsolutions, time_sec])) textBG(0, tt[10+i*space], "Not yet implemented", col=pydtcol, font=2)
  }
  cph = 0.5  # minimum on graph histories; what people will see if they check
  tn = timings[in_rows==.nrow, sum(time_sec, na.rm=TRUE), solution]
  tn = setNames(tn$V1, tn$solution)
  leg = unique(ans1[order(solution)], by="solution"
               )[, tN := tn[solution] # no joining to not reorder
                 ][, sprintf("%s %s  -  %s  -  Total: $%.02f for %s %s",
                             if (solution=="pydatatable") "(py)datatable" else solution, version, # decode pydatatable to (py)datatable
                             solution.date(solution, version, git, only.date=TRUE, use.cache=TRUE),
                             cph*tN/3600, round(tN/timescale, 0), tolower(xlab)), by=solution
                   ][, V1]
  topoffset = 23.8
  legend(0, par()$usr[4]+topoffset*w, pch=22, pt.bg=rev(colors)[-1], bty="n", cex=1.5, pt.cex=3.5,
         text.font=1, xpd=NA, legend=leg)
  mtext(paste("Input table:", comma(.nrow), "rows x 9 columns (",
        if (!is.na(gb)) { if (gb<1) round(gb, 1) else 5*round(ceiling(gb)/5) } else "NA",
        "GB )"),
        side=3, line=6.5, cex=1.5, adj=0, font=2)
  legend(par()$usr[2], par()$usr[4]+topoffset*w, pch=22, xpd=NA, xjust=1, bty="n", pt.lwd=1,
         legend=c("First time", "Second time"), pt.cex=c(3.5, 2.5), cex=1.5, pt.bg=c("blue", lb))
  mtext(side=1, line=-1, text=format(as.POSIXct(vbatch, origin="1970-01-01"), usetz=TRUE), adj=1, outer=TRUE, cex=1)
  dev.off()
  if (interactive()) system(paste("/usr/bin/xdg-open",fnam), wait=FALSE) else invisible(TRUE)
}

if (interactive()) {
  d = fread("time.csv")[!is.na(batch)][batch==max(batch)]
  .nrow=1e9
  timings=d; code=groupby.code; task="groupby"
  #benchplot(.nrow=1e7, timings=d, code=groupby.code)
}
