## Based on Matt Dowle scripts from 2014
## https://github.com/h2oai/db-benchmark/commit/fce1b8c9177afb49471fcf483a438f619f1a992b
## Original grouping benchmark can be found in: https://github.com/Rdatatable/data.table/wiki/Benchmarks-:-Grouping

format_comma = function(x) format(as.integer(signif(x,4)), big.mark=",")
format_num = function(x, digits=3L) { # at least 3+1 chars on output, there is surely some setting to achieve that better with base R but it is not obvious to find that among all features there
  cx = sprintf("%0.2f", x)
  int = sapply(strsplit(cx, ".", fixed=TRUE), `[`, 1L)
  int_digits = sapply(int, nchar)
  stopifnot(int_digits > 0L)
  cx[int_digits == 2L] = substr(cx[int_digits == 2L], 1L, 4L)
  cx[int_digits > 2L] = int[int_digits > 2L]
  cx
}

qbar_pad = function(i, nsolutions) {
  c(as.vector(rbind(
    i*nsolutions+1:nsolutions,  ## solution time bar
    NA)),                       ## solution syntax
    NA,                         ## question header
    NA                          ## axis
  )
}

xlab_labels = function(x) {
  at = pretty(c(0, x), 5, 5)
  at[at > 0 & at < par("usr")[2L]]
}
xlab_timescale = function(x) {
  if (x > 2*60*60) {
    timescale = 3600
    xlab = "Hours"
  } else if (x > 120) {
    timescale = 60
    xlab = "Minutes"
  } else {
    timescale = 1
    xlab = "Seconds"
  }
  setNames(timescale, xlab)
}

text_bg = function(x, y, txt, xpd=NA, col="black", col.bg="white", ..., which=NA) {
  pad = strwidth("w", ...)/2
  w = strwidth(txt, ...) + pad
  h = strheight(txt, ...) + pad
  x = x + w/2 # center
  if (is.na(which)||which=="rect") rect(x-w/2, y-h/2, x+w/2, y+h/2, col=col.bg, border=NA, xpd=xpd)
  if (is.na(which)||which=="text") text(x, y, txt, xpd=xpd, col=col, ...)
  invisible(NULL)
}

bar1 = function(x, pad) {
  invisible(x[pad, barplot(time1, horiz=TRUE, axes=FALSE, col=col_strong, xpd=NA, add=TRUE)])
  invisible(NULL)
}
bar2 = function(x, w) {
  x[, rect(0, bar_y-w, time2, bar_y+w, col=col_light, xpd=NA)]
  invisible(NULL)
}

margins = function(nsolutions) {
  mar.bottom = sum(c(
  ))
  mar.left = sum(c(
    solution_margin = 6
  ))
  mar.top = sum(c(
    title = 1,
    solution_legend = nsolutions,
    spacing = 2.5
  ))
  mar.right = sum(c(
    cutoff_timing_margin = 6
  ))
  par(
    mar=c(mar.bottom, mar.left, mar.top, mar.right),
    oma=c(1,1,1,1)*0
  )
}

xy_fig = function(offset=c(x1=0, x2=0, y1=0, y2=0)) {
  lh = par("cin")[2L] * par("cex") * par("lheight")
  x_off = diff(grconvertX(0:1, "inches", "user"))
  y_off = diff(grconvertY(0:1, "inches", "user"))
  c(
    x1 = par("usr")[1L] - (par("mar")[2L]-offset[["x1"]]) * x_off * lh,
    x2 = par("usr")[2L] + (par("mar")[4L]-offset[["x2"]]) * x_off * lh,
    y1 = par("usr")[3L] - (par("mar")[1L]-offset[["y1"]]) * y_off * lh,
    y2 = par("usr")[4L] + (par("mar")[3L]-offset[["y2"]]) * y_off * lh
  )
}

axis_grid = function(x, nsolutions, nquestions, h1, h2) {
  x_at = x[, xlab_labels(c(time1, time2))]
  # dotted vertical lines to form grid
  for (at_x in x_at) lines(x=c(at_x, at_x), y=c(h1, h2), col="lightgrey", lwd=2, lty="dotted")
  # grey horizontal lines separating questions
  q_line_y = x[, tail(.SD, 1L), question][, syntax_y+(syntax_y-bar_y)*2]
  abline(h=q_line_y, col="grey", lwd=2)
  # upper X axis
  abline(h=h1)
  # upper X ticks
  ff = if (length(x_at)<=8L) TRUE else -1  # ff = first first X axis label overlap, so conditionally exclude
  text(x=x_at[ff], y=h1, labels=format(x_at[ff]), adj=c(0.5, -0.5), cex=1.5, font=2, xpd=NA)
  # upper X axis lab (seconds/minutes)
  timescale = xlab_timescale(x[, max(max_time, na.rm=TRUE)])
  text(x=0, y=h1, labels=names(timescale), adj=c(0, -0.5), cex=1.5, font=2, xpd=NA, pos=3)
  # lower X axis
  abline(h=h2)
  text(x=x_at[ff], y=h2, labels=format(x_at[ff]), adj=c(0.5, 1.5), cex=1.5, font=2, xpd=NA)
  text(x=0, y=h2, labels=names(timescale), adj=c(0, 1.5), cex=1.5, font=2, xpd=NA, pos=1)
  invisible(NULL)
}

default.question.txt.fun = function(x) {
  stopifnot(is.data.table(x), "question" %in% names(x))
  x[["question"]]
}
q_title = function(x, txt.fun=default.question.txt.fun, which=NA) {
  sd = setdiff(names(x), c("question","out_cols","out_rows"))
  d = x[, c(list(out_cols=na.omit(out_cols)[1L], out_rows=na.omit(out_rows)[1L]), tail(.SD, 1L)), question, .SDcols=sd][, c(list(q_title_y=syntax_y+(syntax_y-bar_y)), .SD)]
  d[, text_bg(0, q_title_y, font=2, txt=txt.fun(.SD), which=which)]
}
syntax = function(x, which=NA) x[, text_bg(0, syntax_y, txt=syntax_text, col=col_strong, font=2, which=which)]
values = function(x, which=NA) x[, text_bg(max_time, bar_y, txt=bar_text, cex=1.25, which=which)]
plot_annotations = function(x, question.txt.fun) { # white rect not overlapping text
  q_title(x, txt.fun=question.txt.fun, which="rect")
  syntax(x, which="rect")
  values(x, which="rect")
  q_title(x, txt.fun=question.txt.fun, which="text")
  syntax(x, which="text")
  values(x, which="text")
  invisible(NULL)
}

s_margin = function(x) {
  x[, text(0, bar_y+(syntax_y-bar_y)/2, name_short, col=col_strong, font=2, xpd=NA, pos=2)]
  invisible(NULL)
}

unique1 = function(x) {
  ans = unique(x)
  stopifnot(length(ans)==1L)
  ans
}
data_spec = function(file) {
  fe = !missing(file) && file.exists(file)
  if (fe) {
    gb = file.info(file)$size/1024^3
    gb = if (gb<1) round(gb, 1) else 5*round(ceiling(gb)/5)
    ncol = length(strsplit(system(sprintf("head -1 %s", file), intern=TRUE), ",", fixed=TRUE)[[1L]])
    nrow = as.numeric(strsplit(system(sprintf("wc -l %s", file), intern=TRUE), " ", fixed=TRUE)[[1L]][1L])-1
  } else {
    gb = NA_real_
    ncol = NA_real_
    nrow = NA_real_
  }
  list(gb=gb, ncol=ncol, nrow=nrow)
}
default.title.txt.fun = function(x) {
  stopifnot(is.data.table(x), "data" %in% names(x))
  data_name = unique1(x[["data"]])
  file = file.path("data", paste(data_name, "csv",sep="."))
  ds = data_spec(file)
  sprintf(
    "Input table: %s rows x %s columns ( %s GB )",
    format_comma(as.numeric(ds[["nrow"]])[1L]),
    as.numeric(ds[["ncol"]])[1L],
    as.numeric(ds[["gb"]])[1L]
  )
}
format_exception = function(ex, s, d, q, which=c("data","query")) {
  if (is.na(d)) return("")
  heading = function(x) trimws(sapply(strsplit(x, ":", fixed=TRUE), `[[`, 1L))
  if ("data" %in% which) {
    e = ex$data[[as.character(s)]]
    if (length(e)) {
      this = which(sapply(e, function(ee) any(as.character(d) %in% ee)))[1L]
      if (!is.na(this)) return(heading(names(e[this])))
    }
  }
  if ("query" %in% which) {
    e = ex$query[[as.character(s)]]
    if (length(e)) {
      this = which(sapply(e, function(ee) any(as.character(q) %in% ee)))[1L]
      if (!is.na(this)) return(heading(names(e[this])))
    }
  }
  return("")
}
format_version = function(x) fifelse(is.na(x), "NA", as.character(x))
format_batch = function(x) fifelse(is.na(x), "NA", format(as.Date(as.POSIXct(as.numeric(x), origin="1970-01-01"))))
format_s_total_real_time = function(data, solution, s_questions, s_total_real_time, exceptions) {
  stopifnot(length(solution)==length(s_questions), length(solution)==length(s_total_real_time), length(solution)==length(data),
            is.list(s_questions), is.list(exceptions))
  data = unique1(data)
  na = is.na(s_total_real_time)
  ans = vector("character", length(solution))
  ans[!na] = sprintf("%.0fs", s_total_real_time[!na])
  if (sum(na)) {
    ans[na] = mapply(format_exception, s=solution[na], q=s_questions[na],
                     MoreArgs=list(exceptions=exceptions, data=data))
  }
  ans
}
header_legend = function(x, exceptions=list(), title.txt.fun=default.title.txt.fun) {
  xy = xy_fig(offset = c(x1=0, x2=0, y1=0, y2=1))
  x_at = x[, xlab_labels(c(time1, time2))]
  x_range = c(mean(c(xy[["x1"]], 0)), mean(c(xy[["x2"]], tail(x_at, 1L))))
  x_w = x_range[2L]-x_range[1L]
  x_off = x_range[1L] + seq(0, x_w, by=x_w/99)
  # header title
  h_title = title.txt.fun(x)
  text(x_off[1L], xy[["y2"]], labels=h_title, pos=4, cex=1.5, font=2, xpd=NA)
  # main solution legend
  dt = x[, .(data=unique1(data), version=unique1(version), batch=unique1(batch),
             s_total_real_time=unique1(s_total_real_time), col_strong=unique1(col_strong),
             name_short=unique1(name_short), name_long=unique1(name_long),
             s_questions=list(question)), ## retain all questions so can lookup for exceptions later on
         by="solution"]
  setorderv(dt, "s_total_real_time", na.last=TRUE)
  dt[, legend(x_off[2L], xy[["y2"]], bty="n", cex=1.5,
              pch=22, pt.bg=col_strong, pt.cex=3.5,    ## color square
              text.font=1, xpd=NA,
              legend=name_long)] -> nul                ## solution long name
  dt[, legend(x_off[20L], xy[["y2"]], bty="n", cex=1.5, text.font=1, xpd=NA,
              legend=format_version(version))] -> nul  ## version
  dt[, legend(x_off[40L], xy[["y2"]], bty="n", cex=1.5, text.font=1, xpd=NA,
              legend=format_batch(batch))] -> nul      ## date
  # right aligned total time seconds
  dt[, {
    txt = format_s_total_real_time(data, solution, s_questions, s_total_real_time, exceptions)
    temp = legend(x_off[70L], xy[["y2"]], bty="n", cex=1.5, text.font=1, xpd=NA,
                  legend=rep("", length(txt)),
                  text.width = max(strwidth(txt)))
    text(temp$rect$left + temp$rect$w,
         temp$text$y - 0.3333*(1/log(length(levels(solution)))), # 0.3333 from dd->dev->yCharOffset, 1/log(nsolutions) for better scaling
         txt, pos=2, 
         cex=1.5, xpd=NA)
  }] -> nul                                            ## solution total time
  
  # RHS first second run legend
  yy = mean(c(xy[["y2"]], x[.N, syntax_y]))
  legend(x_off[85L], yy, pch=22, xpd=NA, bty="n", yjust=0,
         pt.lwd=1, cex=1.5, pt.cex=c(3.5, 2.5),
         pt.bg=x[leg_col==TRUE, c(col_strong, col_light)],
         legend=c("First time","Second time"))
  invisible(NULL)
}

footer = function(url.footer) {
  xy = xy_fig(offset = c(x1=0, x2=0, y1=0.5, y2=0))
  # footer link to report
  if (!is.na(url.footer) && nzchar(url.footer)) {
    text(xy[["x1"]], xy[["y1"]], url.footer, pos=4, xpd=NA)
  }
  # footer timestamp of plot gen
  text(xy[["x2"]], xy[["y1"]], format(Sys.time(), usetz=TRUE), pos=2, xpd=NA)
  invisible(NULL)
}

benchplot2 = function(
  x, filename=NULL,
  solution.dict=list(), syntax.dict=list(),
  exceptions=list(),
  cutoff=NULL, cutoff.after=0.2,
  question.txt.fun=default.question.txt.fun,
  title.txt.fun=default.title.txt.fun,
  url.footer = NA_character_,
  interactive = interactive()
) {
  if (!capabilities()[["X11"]] && capabilities()[["cairo"]]) {
    op = options(bitmapType="cairo")
    on.exit(options(op))
  }
  stopifnot(is.data.table(x))
  if (!is.null(filename)) stopifnot(is.character(filename), !is.na(filename), length(filename)==1L)
  if (!is.null(cutoff)) stopifnot(is.character(cutoff), !is.na(cutoff), length(cutoff)==1L, is.numeric(cutoff.after), !is.na(cutoff.after))
  stopifnot(is.list(exceptions), is.list(solution.dict), is.list(syntax.dict))
  stopifnot(is.function(question.txt.fun))
  stopifnot(is.character(url.footer), length(url.footer)==1L)

  x = copy(x)[, i := .I]

  solutions = levels(x$solution)
  nsolutions = length(solutions)
  questions = levels(x$question)
  nquestions = length(questions)
  if (length(cutoff) && !cutoff%in%solutions) stop(sprintf("'cutoff' argument used but provided value '%s' is not a solution existing in timing data", cutoff))
  if (!all(solutions %in% names(solution.dict))) stop("'solution.dict' argument does not define all solutions used")
  if (!all(solutions %in% names(syntax.dict))) stop("'syntax.dict' argument does not define all solutions used")

  x[, c("real_time1","real_time2") := .(time1, time2)]  
  x[, "max_real_time" := max(c(real_time1, real_time2)), by=c("solution", "question")]
  x[, "sum_real_time" := sum(real_time1, real_time2), by=c("solution", "question")]
  x[, "s_total_real_time" := sum(sum_real_time), by=c("solution")]
  # order for bar horiz=TRUE does first bar from the bottom!
  setorderv(x, c("question","max_real_time","solution"), order=-1L, na.last=FALSE)
  x[, c("col_strong","col_light") := as.list(solution.dict[[as.character(solution)]][["color"]]), by="solution"]
  x[, "leg_col" := solution==names(solution.dict[1L])] # first entry from solution.dict is used for color of first-second run legend
  x[, c("name_short","name_long") := as.list(solution.dict[[as.character(solution)]][["name"]]), by="solution"]
  x[, "syntax_text" := as.list(syntax.dict[[as.character(solution)]])[[as.character(question)]], by=c("solution","question")]
  # TODO syntax_text query exceptions only for NA timing

  # bars on Y axis padding
  pad = as.vector(sapply(
    seq.int(nquestions)-1L, # time, solution syntax, and top X axis and its labels
    qbar_pad, nsolutions = nsolutions
  ))
  if (!is.null(filename)) {
    height = sum(c(   ## approximately
      30,             # header
      30*nsolutions,  # legend solutions
      30,             # top X axis labels
      nquestions*(nsolutions+1)*30, # +1 for some spacing somewhere
      30,             # bottom X axis labels
      30              # footer
    ))
    png(filename=filename, width=800, height=height)
  }
  margins(nsolutions)
  x[, c("cutoff1","cutoff2") := .(FALSE, FALSE)]
  if (length(cutoff)) {
    if (!cutoff%in%solutions) stop(sprintf("'cutoff' argument used but provided value '%s' is not a solution existing in timing data", cutoff))
    cutoff_const = x[solution==cutoff, max(max_real_time)*(1+cutoff.after)]
    if (is.na(cutoff_const)) cutoff_const = +Inf
    x[time1>cutoff_const, `:=`(cutoff=TRUE, time1=cutoff_const)]
    x[time2>cutoff_const, `:=`(cutoff=TRUE, time2=cutoff_const)]
  }
  x[, "max_time" := max(c(time1, time2)), by=c("solution","question")]
  # get bars Y coordinates, positions only, plot later in bar1
  all_y_bars = barplot(rep(NA_real_, length(pad)), horiz=TRUE, xlim=c(0, max(x$max_time)), axes=FALSE, xpd=FALSE)
  box("plot", col="black") ## temporary for development
  box("figure", col="blue")
  box("inner", col="orange")
  box("outer", col="red")
  bar_step = all_y_bars[2L]-all_y_bars[1L]
  x[, "bar_y" := all_y_bars[!is.na(pad)]]
  x[, "syntax_y" := all_y_bars[which(!is.na(pad))+1L]]
  x[, "bar_text" := paste(format_num(c(real_time1, real_time2)), collapse="; "), by=c("solution","question")]
  x[cutoff==TRUE, "bar_text" := paste("...", bar_text)]

  header_legend(x, exceptions=exceptions, title.txt.fun=title.txt.fun)
  axis_grid(x, nsolutions, nquestions, h1=all_y_bars[length(all_y_bars)], h2=all_y_bars[1L]-bar_step)
  plot_annotations(x, question.txt.fun=question.txt.fun)
  bar1(x, pad)
  bar2(x, bar_step/4)
  s_margin(x)
  footer(url.footer)
  if (!is.null(filename)) {
    dev.off()
    if (interactive) system(paste("/usr/bin/xdg-open", filename), wait=FALSE)
  }
  invisible(NULL)
}
