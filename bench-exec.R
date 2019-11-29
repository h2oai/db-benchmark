# filter nsolutions nquestions ----

filter = function(ld, nsolutions = 9L, nquestions = 5L) {
  stopifnot(nsolutions >= 2L, nquestions >= 1L)
  ## solutions filter
  ld = ld[solution %in% head(levels(ld$solution), n=nsolutions)]
  ## questions filter
  ld = ld[question %in% head(as.character(unique(ld$question)), n=nquestions)]
  ld
}

# benchplot calls ----

old = function(file) local({
  source("benchplot-dict.R")
  source("benchplot.R")
  benchplot(
    .nrow = substr(d, 4L, 6L),
    task=t, data=d,
    timings = copy(ld),
    cutoff = if ("spark"%in%as.character(unique(ld$solution))) "spark" else character(),
    code=groupby.code, exceptions=groupby.exceptions, colors=solution.colors, .interactive=FALSE, fnam=file, path="."
  )
})
new = function(file) local({
  source("benchplot-dict2.R")
  source("benchplot2.R")
  x = ld[data==d]
  benchplot2(
    x, filename = file,
    solution.dict = solution.dict,
    syntax.dict = groupby.syntax.dict,
    exceptions = groupby.exceptions,
    question.txt.fun = groupby_q_title_fun,  
    title.txt.fun = header_title_fun,
    cutoff = "spark",
    pending = "Modin",
    url.footer = "https://h2oai.github.io/db-benchmark",
    interactive = FALSE
  )
})

# run ----

library(data.table)
source("report.R")
t = "groupby"
d = "G1_1e7_1e2_0_0"
q_group = "basic"
ldd = time_logs()[task==t & script_recent==TRUE & question_group==q_group]

system("pkill feh", wait=TRUE)
ld = filter(ldd, nsolutions=9L)
system("rm -f b1_9.png b2_9.png")
old(file="b1_9.png")
new(file="b2_9.png")
system("feh -w b1_9.png b2_9.png", wait=FALSE)

system("pkill feh", wait=TRUE)
ld = filter(ldd, nsolutions=3L)
system("rm -f b1_3.png b2_3.png")
old(file="b1_3.png")
new(file="b2_3.png")
system("feh -w b1_3.png b2_3.png", wait=FALSE)

# scale for solutions
system("pkill feh", wait=TRUE)
system("rm -f b2_3.png b2_6.png b2_9.png")
ld = filter(ldd, nsolutions=9L)
new(file="b2_9.png")
ld = filter(ldd, nsolutions=6L)
new(file="b2_6.png")
ld = filter(ldd, nsolutions=3L)
new(file="b2_3.png")
system("feh -w b2_3.png b2_6.png b2_9.png", wait=FALSE)

# todo ----

# - [x] vectorized code, avoid loops, keep more information inside the data
# - [x] footer alignement in corner
# - [x] avoid to many ticks on X axis
# - [x] X axis cutoff to early
# - [x] timings in legend overlaps RHS run legend
# - [x] headers more adjustable from functions (support various tasks)
# - [x] syntax dict stacked by solution, not question
# - [x] white background of text should not overlap another text
# - [x] solution colors and short/long names moved to dictionary
# - [x] isolate parts of the plot into own functions for readability and maintenance
# - [x] handling of non present cutoff solution
# - [x] pending entry in legend
# - [x] legend left maring
# - [x] first/second run legend y location more stable
# - [x] solution names on lhs margin and legend
# - [x] overlapping grid, axes to textBG
# - [x] exceptions
# - [x] question headers
# - [x] proper cutoff at the edge
# - [x] syntax_text query exceptions only for NA timing
# - [x] support for a all non fully sucessful solutions timings (none of solutions finished all questions)
# - [x] scale for solutions (3-9)
# - [x] minutes-seconds translation error
# - [-] scale for questions (3-9)
# - [-] scale for s*q (3*3, 3*10, 9*3, 9*10)
# - [x] order of exception solutions on legend
# - [x] join test
# - [x] plot bar only if both runs successful
# - [x] test whole report and every single benchplot vs prod
# - [ ] speed test
# - [ ] update scripts for DT/DF/x

# visible changes:
# - question headers (question, more precise stats)
# - consistent x/DF/DT naming
# - handling of single solution
# - number of ticks on X axis capped to more pretty
# - better scaling for various number of solutions
# - timing values might had sometimes `...` prefix even when they fit the X lim plot, it is now fixed
# - even if timings of cutoff solution (spark) doesn't exists plot will procedure without cutoff

# join

library(data.table)
source("report.R")
t = "join"
d = "J1_1e9_NA_0_0"
q_group = "basic"
ldd = time_logs()[task==t & script_recent==TRUE & question_group==q_group]

oldj = function(file) local({
  source("benchplot-dict.R")
  source("benchplot.R")
  benchplot(
    .nrow = substr(d, 4L, 6L),
    task=t, data=d,
    timings = copy(ld),
    cutoff = if ("spark"%in%as.character(unique(ld$solution))) "spark" else character(),
    code=join.code, exceptions=join.exceptions, colors=solution.colors, .interactive=FALSE, fnam=file, path="."
  )
})
newj = function(file) local({
  source("benchplot-dict2.R")
  source("benchplot2.R")
  x = ld[data==d]
  f = sapply(x, is.factor)
  x[, names(x)[f] := lapply(.SD, factor), .SDcols=f]
  setnames(x, c("time_sec_1","time_sec_2"), c("time1","time2"))
  benchplot2(
    x, filename = file,
    solution.dict = solution.dict,
    syntax.dict = join.syntax.dict,
    exceptions = join.exceptions,
    question.txt.fun = join_q_title_fun,
    title.txt.fun = header_title_fun,
    cutoff = "spark",
    pending = c("Modin","ClickHouse"),
    url.footer = "https://h2oai.github.io/db-benchmark",
    interactive = FALSE
  )
})

system("pkill feh", wait=TRUE)
ld = filter(ldd, nsolutions=9L)
system("rm -f b1_8.png b2_8.png")
oldj(file="b1_8.png")
newj(file="b2_8.png")
system("feh -w b1_8.png b2_8.png", wait=FALSE)
