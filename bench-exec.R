library(data.table)
source("report.R")
q_group = "basic"
t = "groupby"
d = "G1_1e7_1e2_0_0"
ld = time_logs()[task==t & script_recent==TRUE & question_group==q_group]

# nsolutions nquestions ----

nsolutions = 3L
nquestions = 5L
stopifnot(nsolutions >= 2L, nquestions >= 1L)

## solutions filter
s = head(c("data.table","dplyr","pandas","pydatatable","spark","dask","juliadf","clickhouse","cudf"), n=nsolutions)
ld = ld[solution %in% s]
cutoff = if ("spark"%in%s) "spark" else if ("pandas"%in%s) "pandas" else s[2L]
## questions filter
ld = ld[question %in% head(levels(ld$question), n=nquestions)]

# benchplot ----

old = T
stopifnot(!old || nquestions==5L)
if (old) {
  source("benchplot-dict.R")
  source("benchplot.R")
  benchplot(
    .nrow = substr(d, 4L, 6L),
    task=t, data=d,
    timings = copy(ld),
    cutoff = cutoff,
    code=groupby.code, exceptions=groupby.exceptions, colors=solution.colors, .interactive=FALSE, fnam="b1.png", path="."
  )
} else {
  source("benchplot-dict2.R")
  source("benchplot2.R")
  x = ld[data==d]
  f = sapply(x, is.factor)
  x[, names(x)[f] := lapply(.SD, factor), .SDcols=f]
  setnames(x, c("time_sec_1","time_sec_2"), c("time1","time2"))
  benchplot2(
    x, filename = "b2.png",
    solution.dict = groupby.solution.dict,
    syntax.dict = groupby.syntax.dict,
    exceptions = groupby.exceptions,
    question.txt.fun = groupby_q_title_fun,  
    title.txt.fun = header_title_fun,
    cutoff = cutoff,
    url.footer = "https://h2oai.github.io/db-benchmark",
    interactive = FALSE
  )
}

# system("feh -w b1.png b2.png", wait=FALSE)

## TODO
# - [x] vectorized code, avoid loops, keep more information inside the data
# - [x] footer alignement in corner
# - [x] avoid to many ticks on X axis
# - [x] headers more adjustable from functions (support various tasks)
# - [x] syntax dict stacked by solution, not question
# - [x] white background of text should not overlap another text
# - [x] solution colors and short/long names moved to dictionary
# - [x] isolate parts of the plot into own functions for readability and maintenance
# - [ ] pending entry in legend
# - [x] legend left maring
# - [ ] first/second run legend little bit lower
# - [x] solution names on lhs margin and legend
# - [ ] X axis cutoff to early
# - [ ] question headers
# - [ ] syntax_text query exceptions only for NA timing
# - [ ] support for a all non fully sucessful solutions timings (none of solutions finished all questions)
# - [ ] scale for solutions (2-10)
# - [ ] scale for questions (2-10)
# - [ ] scale for s*q (2*2, 2*10, 10*2, 10*10)
