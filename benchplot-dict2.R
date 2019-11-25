task.exceptions = function(query, data) {
  ex = list(query = query, data = data)
  unq_in_list = function(x) {
    y = unlist(x, use.names=FALSE)
    length(unique(y))==length(y)
  }
  if (!all(sapply(ex$query, unq_in_list))) stop("task.exceptions detected invalid entries in 'query' exceptions")
  if (!all(sapply(ex$data, unq_in_list))) stop("task.exceptions detected invalid entries in 'data' exceptions")
  ex
}

groupby_q_title_fun = function(x) {
  stopifnot(c("iquestion","out_rows","out_cols","in_rows") %in% names(x))
  x[, sprintf("Question %s: %s ad hoc groups of %s rows;  result %s x %s", iquestion, format_comma(out_rows), format_comma(as.numeric(as.character(in_rows))/as.numeric(out_rows)), format_comma(out_rows), out_cols)]
}
join_q_title_fun = function(x) {
  stopifnot(c("iquestion","out_rows","out_cols") %in% names(x))
  x[, sprintf("Question %s: result %s x %s", iquestion, format_comma(out_rows), out_cols)]
}
header_title_fun = function(x) {
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

groupby.solution.dict = list(
  "data.table" = list(name=c(short="data.table", long="data.table"), color=c(strong="blue", light="#7777FF")),
  "dplyr" = list(name=c(short="dplyr", long="dplyr"), color=c(strong="red", light="#FF7777")),
  "pandas" = list(name=c(short="pandas", long="pandas"), color=c(strong="green4", light="#77FF77")),
  "pydatatable" = list(name=c(short="pydatatable", long="(py)datatable"), color=c(strong="darkorange", light="orange")),
  "spark" = list(name=c(short="spark", long="spark"), color=c(strong="#8000FFFF", light="#CC66FF")),
  "dask" = list(name=c(short="dask", long="dask"), color=c(strong="slategrey", light="lightgrey")),
  "juliadf" = list(name=c(short="DF.jl", long="DataFrames.jl"), color=c(strong="deepskyblue", light="darkturquoise")),
  "clickhouse" = list(name=c(short="clickhouse", long="ClickHouse"), color=c(strong="hotpink4", light="hotpink1")),
  "cudf" = list(name=c(short="cuDF", long="cuDF"), color=c(strong="peachpuff3", light="peachpuff1"))
)

#sapply(sapply(groupby.code, `[[`, 1L), cat, "\n") -> nul
groupby.syntax.dict = list(
  "data.table" = {c(
    "sum v1 by id1" = "DT[, .(v1=sum(v1)), by=id1]",
    "sum v1 by id1:id2" = "DT[, .(v1=sum(v1)), by=.(id1, id2)]",
    "sum v1 mean v3 by id3" = "DT[, .(v1=sum(v1), v3=mean(v3)), by=id3]",
    "mean v1:v3 by id4" = "DT[, lapply(.SD, mean), by=id4, .SDcols=v1:v3]",
    "sum v1:v3 by id6" = "DT[, lapply(.SD, sum), by=id6, .SDcols=v1:v3]"
  )},
  "dplyr" = {c(
    "sum v1 by id1" = "DF %>% group_by(id1, .drop=TRUE) %>% summarise(sum(v1))",
    "sum v1 by id1:id2" = "DF %>% group_by(id1, id2, .drop=TRUE) %>% summarise(sum(v1))",
    "sum v1 mean v3 by id3" = "DF %>% group_by(id3, .drop=TRUE) %>% summarise(sum(v1), mean(v3))",
    "mean v1:v3 by id4" = "DF %>% group_by(id4, .drop=TRUE) %>% summarise_each(funs(mean), vars=7:9)",
    "sum v1:v3 by id6" = "DF %>% group_by(id6, .drop=TRUE) %>% summarise_each(funs(sum), vars=7:9)"
  )},
  "pandas" = {c(
    "sum v1 by id1" = "DF.groupby(['id1']).agg({'v1':'sum'})",
    "sum v1 by id1:id2" = "DF.groupby(['id1','id2']).agg({'v1':'sum'})",
    "sum v1 mean v3 by id3" = "DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})",
    "mean v1:v3 by id4" = "DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})",
    "sum v1:v3 by id6" = "DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})"
  )},
  "pydatatable" = {c(
    "sum v1 by id1" = "DT[:, {'v1': sum(f.v1)}, by(f.id1)]",
    "sum v1 by id1:id2" = "DT[:, {'v1': sum(f.v1)}, by(f.id1, f.id2)]",
    "sum v1 mean v3 by id3" = "DT[:, {'v1': sum(f.v1), 'v3': mean(f.v3)}, by(f.id3)]",
    "mean v1:v3 by id4" = "DT[:, {'v1': mean(f.v1), 'v2': mean(f.v2), 'v3': mean(f.v3)}, by(f.id4)]",
    "sum v1:v3 by id6" = "DT[:, {'v1': sum(f.v1), 'v2': sum(f.v2), 'v3': sum(f.v3)}, by(f.id6)]"
  )},
  "dask" = {c(
    "sum v1 by id1" = "DF.groupby(['id1']).agg({'v1':'sum'}).compute()",
    "sum v1 by id1:id2" = "DF.groupby(['id1','id2']).agg({'v1':'sum'}).compute()",
    "sum v1 mean v3 by id3" = "DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'}).compute()",
    "mean v1:v3 by id4" = "DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()",
    "sum v1:v3 by id6" = "DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()"
  )},
  "spark" = {c(
    "sum v1 by id1" = "spark.sql('select id1, sum(v1) as v1 from x group by id1')",
    "sum v1 by id1:id2" = "spark.sql('select id1, id2, sum(v1) as v1 from x group by id1, id2')",
    "sum v1 mean v3 by id3" = "spark.sql('select id3, sum(v1) as v1, mean(v3) as v3 from x group by id3')",
    "mean v1:v3 by id4" = "spark.sql('select id4, mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from x group by id4')",
    "sum v1:v3 by id6" = "spark.sql('select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6')"
  )},
  "juliadf" = {c(
    "sum v1 by id1" = "by(DF, :id1, v1 = :v1 => sum) ",
    "sum v1 by id1:id2" = "by(DF, [:id1, :id2], v1 = :v1 => sum) ",
    "sum v1 mean v3 by id3" = "by(DF, :id3, v1 = :v1 => sum, v3 = :v3 => mean) ",
    "mean v1:v3 by id4" = "by(DF, :id4, v1 = :v1 => mean, v2 = :v2 => mean, v3 = :v3 => mean) ",
    "sum v1:v3 by id6" = "by(DF, :id6, v1 = :v1 => sum, v2 = :v2 => sum, v3 = :v3 => sum) "
  )},
  "cudf" = {c(
    "sum v1 by id1" = "DF.groupby(['id1'],as_index=False).agg({'v1':'sum'})",
    "sum v1 by id1:id2" = "DF.groupby(['id1','id2'],as_index=False).agg({'v1':'sum'})",
    "sum v1 mean v3 by id3" = "DF.groupby(['id3'],as_index=False).agg({'v1':'sum', 'v3':'mean'})",
    "mean v1:v3 by id4" = "DF.groupby(['id4'],as_index=False).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})",
    "sum v1:v3 by id6" = "DF.groupby(['id6'],as_index=False).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})"
  )},
  "clickhouse" = {c(
    "sum v1 by id1" = "SELECT id1, sum(v1) AS v1 FROM x GROUP BY id1",
    "sum v1 by id1:id2" = "SELECT id1, id2, sum(v1) AS v1 FROM x GROUP BY id1, id2",
    "sum v1 mean v3 by id3" = "SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM x GROUP BY id3",
    "mean v1:v3 by id4" = "SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM x GROUP BY id4",
    "sum v1:v3 by id6" = "SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM x GROUP BY id6"
  )}
)

groupby.query.exceptions = {list(
  "data.table" =  list(),
  "dplyr" =       list(),
  "pandas" =      list(),
  "pydatatable" = list(),
  "spark" =       list("not yet implemented: SPARK-26589" = "median v3 sd v3 by id4 id5"),
  "dask" =        list("not yet implemented: dask#4362" = "median v3 sd v3 by id4 id5",
                       "not yet documented: dask#5622" = "regression v1 v2 by id2 id4"), #122
  "juliadf" =     list(),
  "cudf" =        list("not yet released: cudf#1085" = "median v3 sd v3 by id4 id5", #121
                       "not yet implemented: cudf#2591" = "max v1 - min v2 by id3",
                       "not yet implemented: cudf#2592" = "largest two v3 by id6",
                       "not yet implemented: cudf#1267" = "regression v1 v2 by id2 id4"),
  "clickhouse" =  list()
)}
groupby.data.exceptions = {list(                                                             # exceptions as of run 1573882448
  "data.table" = {list(
    "segfault" = c("G1_1e9_2e0_0_0"),                                                        # fread # before it was q3 #110
    "timeout" = c("G1_1e9_1e1_0_0")                                                          # q8 probably #110
  )},
  "dplyr" = {list(
    "timeout" = c("G1_1e7_1e2_0_0","G1_1e7_1e1_0_0","G1_1e7_2e0_0_0","G1_1e7_1e2_0_1",       # q10
                  "G1_1e8_1e2_0_0","G1_1e8_1e1_0_0","G1_1e8_2e0_0_0","G1_1e8_1e2_0_1",       # q10
                  "G1_1e9_1e2_0_0","G1_1e9_1e2_0_1",                                         # q10
                  "G1_1e9_1e1_0_0"),                                                         # q6
    "segfault" = c("G1_1e9_2e0_0_0")                                                         # fread # before it was q3 #110
  )},
  "pandas" = {list(
    "out of memory" = c("G1_1e7_1e2_0_0","G1_1e7_1e1_0_0","G1_1e7_2e0_0_0","G1_1e7_1e2_0_1"),# q10
    "timeout" = c("G1_1e8_1e2_0_0","G1_1e8_1e1_0_0","G1_1e8_2e0_0_0","G1_1e8_1e2_0_1"),      # q10
    "out of memory" = c("G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1") # read_csv #99
  )},
  "pydatatable" = {list(
  )},
  "spark" = {list(
  )},
  "dask" = {list(
    "timeout" = c("G1_1e7_2e0_0",                                                  # q8
                  "G1_1e8_1e2_0_0",                                                # q8
                  "G1_1e8_1e1_0_0",                                                # q8
                  "G1_1e8_2e0_0_0",                                                # q8
                  "G1_1e8_1e2_0_1"),                                               # q8
    "segfault" = c("G1_1e9_1e2_0_1"),                                              # read_csv
    "out of memory" = c("G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0")        # read_csv  #99
  )},
  "juliadf" = {list(
    "out of memory" = c("G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1") # CSV.File
  )},
  "cudf" = {list(
    "out of memory" = c("G1_1e8_1e2_0_0","G1_1e8_1e1_0_0","G1_1e8_2e0_0_0","G1_1e8_1e2_0_1", # read_csv #94
                        "G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1") # read_csv #97
  )},
  "clickhouse" = {list(
    "CH server crash" = c("G1_1e9_1e2_0_0",                                        # q10 #112 before it was #96
                          "G1_1e9_1e1_0_0",                                        # q7 #112
                          "G1_1e9_2e0_0_0",                                        # q3 #112
                          "G1_1e9_1e2_0_1")                                        # q10 #112 before it was #96
  )}
)}
groupby.exceptions = task.exceptions(groupby.query.exceptions, groupby.data.exceptions)
