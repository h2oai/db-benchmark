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
header_title_fun = function(x) {
  stopifnot(is.data.table(x), "data" %in% names(x))
  data_name = unique1(x[["data"]])
  file = file.path("data", paste(data_name, "csv",sep="."))
  ds = data_spec(file, nrow=as.numeric(substr(data_name, 4L, 6L)))
  sprintf(
    "Input table: %s rows x %s columns ( %s GB )",
    format_comma(as.numeric(ds[["nrow"]])[1L]),
    as.numeric(ds[["ncol"]])[1L],
    as.numeric(ds[["gb"]])[1L]
  )
}
solution.dict = {list(
  "data.table" = list(name=c(short="data.table", long="data.table"), color=c(strong="blue", light="#7777FF")),
  "dplyr" = list(name=c(short="dplyr", long="dplyr"), color=c(strong="red", light="#FF7777")),
  "pandas" = list(name=c(short="pandas", long="pandas"), color=c(strong="green4", light="#77FF77")),
  "pydatatable" = list(name=c(short="pydatatable", long="(py)datatable"), color=c(strong="darkorange", light="orange")),
  "spark" = list(name=c(short="spark", long="spark"), color=c(strong="#8000FFFF", light="#CC66FF")),
  "dask" = list(name=c(short="dask", long="dask"), color=c(strong="slategrey", light="lightgrey")),
  "juliadf" = list(name=c(short="DF.jl", long="DataFrames.jl"), color=c(strong="deepskyblue", light="darkturquoise")),
  "clickhouse" = list(name=c(short="clickhouse", long="ClickHouse"), color=c(strong="hotpink4", light="hotpink1")),
  "cudf" = list(name=c(short="cuDF", long="cuDF"), color=c(strong="peachpuff3", light="peachpuff1"))
)}

# groupby ----

groupby_q_title_fun = function(x) {
  stopifnot(c("question","iquestion","out_rows","out_cols","in_rows") %in% names(x),
            uniqueN(x, by="iquestion")==nrow(x))
  x = copy(x)[, "top2":=FALSE][, "iquestion":=rev(seq_along(iquestion))]
  x[question=="largest two v3 by id6", "top2":=TRUE] #118
  x[, sprintf("Question %s: \"%s\": %s%s ad hoc groups of ~%s rows;  result %s x %s",
              iquestion, as.character(question),
              if (top2) "~" else "",
              format_comma(if (top2) out_rows/2 else out_rows),
              if (top2) "2" else format_comma(as.numeric(as.character(in_rows))/as.numeric(out_rows)),
              format_comma(out_rows), out_cols),
    by = "iquestion"]$V1
}
groupby.syntax.dict = {list(
  "data.table" = {c(
    "sum v1 by id1" = "DT[, .(v1=sum(v1)), by=id1]",
    "sum v1 by id1:id2" = "DT[, .(v1=sum(v1)), by=.(id1, id2)]",
    "sum v1 mean v3 by id3" = "DT[, .(v1=sum(v1), v3=mean(v3)), by=id3]",
    "mean v1:v3 by id4" = "DT[, lapply(.SD, mean), by=id4, .SDcols=v1:v3]",
    "sum v1:v3 by id6" = "DT[, lapply(.SD, sum), by=id6, .SDcols=v1:v3]",
    "median v3 sd v3 by id4 id5" = "DT[, .(median_v3=median(v3), sd_v3=sd(v3)), by=.(id4, id5)]",
    "max v1 - min v2 by id3" = "DT[, .(range_v1_v2=max(v1)-min(v2)), by=id3]",
    "largest two v3 by id6" = "DT[order(-v3), .(largest2_v3=head(v3, 2L)), by=id6]",
    "regression v1 v2 by id2 id4" = "DT[, .(r2=cor(v1, v2)^2), by=.(id2, id4)]",
    "sum v3 count by id1:id6" = "DT[, .(v3=sum(v3), count=.N), by=id1:id6]"
  )},
  "dplyr" = {c(
    "sum v1 by id1" = "DF %>% group_by(id1, .drop=TRUE) %>% summarise(sum(v1))",
    "sum v1 by id1:id2" = "DF %>% group_by(id1, id2, .drop=TRUE) %>% summarise(sum(v1))",
    "sum v1 mean v3 by id3" = "DF %>% group_by(id3, .drop=TRUE) %>% summarise(sum(v1), mean(v3))",
    "mean v1:v3 by id4" = "DF %>% group_by(id4, .drop=TRUE) %>% summarise_each(funs(mean), vars=7:9)",
    "sum v1:v3 by id6" = "DF %>% group_by(id6, .drop=TRUE) %>% summarise_each(funs(sum), vars=7:9)",
    "median v3 sd v3 by id4 id5" = "DF %>% group_by(id4, id5, .drop=TRUE) %>% summarise(median_v3=median(v3), sd_v3=sd(v3))",
    "max v1 - min v2 by id3" = "DF %>% group_by(id3, .drop=TRUE) %>% summarise(range_v1_v2=max(v1)-min(v2))",
    "largest two v3 by id6" = "DF %>% select(id6, largest2_v3=v3) %>% arrange(desc(largest2_v3)) %>% group_by(id6, .drop=TRUE) %>% filter(row_number() <= 2L)",
    "regression v1 v2 by id2 id4" = "DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(r2=cor(v1, v2)^2)",
    "sum v3 count by id1:id6" = "DF %>% group_by(id1, id2, id3, id4, id5, id6, .drop=TRUE) %>% summarise(v3=sum(v3), count=n())"
  )},
  "pandas" = {c(
    "sum v1 by id1" = "DF.groupby(['id1']).agg({'v1':'sum'})",
    "sum v1 by id1:id2" = "DF.groupby(['id1','id2']).agg({'v1':'sum'})",
    "sum v1 mean v3 by id3" = "DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})",
    "mean v1:v3 by id4" = "DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})",
    "sum v1:v3 by id6" = "DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})",
    "median v3 sd v3 by id4 id5" = "DF.groupby(['id4','id5']).agg({'v3': ['median','std']})",
    "max v1 - min v2 by id3" = "DF.groupby(['id3']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']]",
    "largest two v3 by id6" = "DF[['id6','v3']].sort_values('v3', ascending=False).groupby(['id6']).head(2)",
    "regression v1 v2 by id2 id4" = "DF[['id2','id4','v1','v2']].groupby(['id2','id4']).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))",
    "sum v3 count by id1:id6" = "DF.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'})"
  )},
  "pydatatable" = {c(
    "sum v1 by id1" = "DT[:, {'v1': sum(f.v1)}, by(f.id1)]",
    "sum v1 by id1:id2" = "DT[:, {'v1': sum(f.v1)}, by(f.id1, f.id2)]",
    "sum v1 mean v3 by id3" = "DT[:, {'v1': sum(f.v1), 'v3': mean(f.v3)}, by(f.id3)]",
    "mean v1:v3 by id4" = "DT[:, {'v1': mean(f.v1), 'v2': mean(f.v2), 'v3': mean(f.v3)}, by(f.id4)]",
    "sum v1:v3 by id6" = "DT[:, {'v1': sum(f.v1), 'v2': sum(f.v2), 'v3': sum(f.v3)}, by(f.id6)]",
    "median v3 sd v3 by id4 id5" = "DT[:, {'median_v3': median(f.v3), 'sd_v3': sd(f.v3)}, by(f.id4, f.id5)]",
    "max v1 - min v2 by id3" = "DT[:, {'range_v1_v2': max(f.v1)-min(f.v2)}, by(f.id3)]",
    "largest two v3 by id6" = "DT[:2, {'largest2_v3': f.v3}, by(f.id6), sort(-f.v3)]",
    "regression v1 v2 by id2 id4" = "DT[:, {'r2': corr(f.v1, f.v2)**2}, by(f.id2, f.id4)]",
    "sum v3 count by id1:id6" = "DT[:, {'v3': sum(f.v3), 'count': count()}, by(f.id1, f.id2, f.id3, f.id4, f.id5, f.id6)]"
  )},
  "dask" = {c(
    "sum v1 by id1" = "DF.groupby(['id1']).agg({'v1':'sum'}).compute()",
    "sum v1 by id1:id2" = "DF.groupby(['id1','id2']).agg({'v1':'sum'}).compute()",
    "sum v1 mean v3 by id3" = "DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'}).compute()",
    "mean v1:v3 by id4" = "DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()",
    "sum v1:v3 by id6" = "DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()",
    "median v3 sd v3 by id4 id5" = "",
    "max v1 - min v2 by id3" = "DF.groupby(['id3']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']].compute()",
    "largest two v3 by id6" = "DF[['id6','v3']].groupby(['id6']).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id6': 'int64', 'v3': 'float64'})[['v3']].compute()",
    "regression v1 v2 by id2 id4" = "",
    "sum v3 count by id1:id6" = "DF.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'}).compute()"
  )},
  "spark" = {c(
    "sum v1 by id1" = "spark.sql('select id1, sum(v1) as v1 from x group by id1')",
    "sum v1 by id1:id2" = "spark.sql('select id1, id2, sum(v1) as v1 from x group by id1, id2')",
    "sum v1 mean v3 by id3" = "spark.sql('select id3, sum(v1) as v1, mean(v3) as v3 from x group by id3')",
    "mean v1:v3 by id4" = "spark.sql('select id4, mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from x group by id4')",
    "sum v1:v3 by id6" = "spark.sql('select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6')",
    "median v3 sd v3 by id4 id5" = "",
    "max v1 - min v2 by id3" = "spark.sql('select id3, max(v1)-min(v2) as range_v1_v2 from x group by id3')",
    "largest two v3 by id6" = "spark.sql('select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from x) sub_query where order_v3 <= 2')",
    "regression v1 v2 by id2 id4" = "spark.sql('select id2, id4, pow(corr(v1, v2), 2) as r2 from x group by id2, id4')",
    "sum v3 count by id1:id6" = "spark.sql('select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6')"
  )},
  "juliadf" = {c(
    "sum v1 by id1" = "combine(groupby(DF, :id1), :v1 => sum => :v1)",
    "sum v1 by id1:id2" = "combine(groupby(DF, [:id1, :id2]), :v1 => sum => :v1)",
    "sum v1 mean v3 by id3" = "combine(groupby(DF, :id3), :v1 => sum => :v1, :v3 => mean => :v3)",
    "mean v1:v3 by id4" = "combine(groupby(DF, :id4), :v1 => mean => :v1, :v2 => mean => :v2, :v3 => mean => :v3)",
    "sum v1:v3 by id6" = "combine(groupby(DF, :id6), :v1 => sum => :v1, :v2 => sum => :v2, :v3 => sum => :v3)",
    "median v3 sd v3 by id4 id5" = "combine(groupby(DF, [:id4, :id5]), :v3 => median => :median_v3, :v3 => std => :sd_v3)",
    "max v1 - min v2 by id3" = "combine(groupby(DF, :id3), [:v1, :v2] => ((v1, v2) -> maximum(v1)-minimum(v2)) => :range_v1_v2)",
    "largest two v3 by id6" = "combine(groupby(DF, :id6), :v3 => (x -> partialsort(x, 1:min(2, length(x)), rev=true)) => :largest2_v3)",
    "regression v1 v2 by id2 id4" = "combine(groupby(DF, [:id2, :id4]), [:v1, :v2] => ((v1,v2) -> cor(v1, v2)^2) => :r2)",
    "sum v3 count by id1:id6" = "combine(groupby(DF, [:id1, :id2, :id3, :id4, :id5, :id6]), :v3 => sum => :v3, :v3 => length => :count)"
  )},
  "cudf" = {c(
    "sum v1 by id1" = "DF.groupby(['id1'],as_index=False).agg({'v1':'sum'})",
    "sum v1 by id1:id2" = "DF.groupby(['id1','id2'],as_index=False).agg({'v1':'sum'})",
    "sum v1 mean v3 by id3" = "DF.groupby(['id3'],as_index=False).agg({'v1':'sum', 'v3':'mean'})",
    "mean v1:v3 by id4" = "DF.groupby(['id4'],as_index=False).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})",
    "sum v1:v3 by id6" = "DF.groupby(['id6'],as_index=False).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})",
    "median v3 sd v3 by id4 id5" = "",
    "max v1 - min v2 by id3" = "",
    "largest two v3 by id6" = "",
    "regression v1 v2 by id2 id4" = "",
    "sum v3 count by id1:id6" = "DF.groupby(['id1','id2','id3','id4','id5','id6'],as_index=False).agg({'v3':'sum', 'v1':'count'})"
  )},
  "clickhouse" = {c(
    "sum v1 by id1" = "SELECT id1, sum(v1) AS v1 FROM x GROUP BY id1",
    "sum v1 by id1:id2" = "SELECT id1, id2, sum(v1) AS v1 FROM x GROUP BY id1, id2",
    "sum v1 mean v3 by id3" = "SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM x GROUP BY id3",
    "mean v1:v3 by id4" = "SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM x GROUP BY id4",
    "sum v1:v3 by id6" = "SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM x GROUP BY id6",
    "median v3 sd v3 by id4 id5" = "SELECT id4, id5, medianExact(v3) AS median_v3, stddevPop(v3) AS sd_v3 FROM x GROUP BY id4, id5",
    "max v1 - min v2 by id3" = "SELECT id3, max(v1) - min(v2) AS range_v1_v2 FROM x GROUP BY id3",
    "largest two v3 by id6" = "SELECT id6, arrayJoin(arraySlice(arrayReverseSort(groupArray(v3)), 1, 2)) AS v3 FROM x GROUP BY id6",
    "regression v1 v2 by id2 id4" = "SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4",
    "sum v3 count by id1:id6" = "SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count() AS cnt FROM x GROUP BY id1, id2, id3, id4, id5, id6"
  )}
)}
groupby.query.exceptions = {list(
  "data.table" =  list(),
  "dplyr" =       list(),
  "pandas" =      list(),
  "pydatatable" = list(),
  "spark" =       list("not yet implemented: SPARK-26589" = "median v3 sd v3 by id4 id5"),
  "dask" =        list("not yet implemented: dask#4362" = "median v3 sd v3 by id4 id5",
                       "not yet documented: dask#5622" = "regression v1 v2 by id2 id4"), #122
  "juliadf" =     list(),
  "cudf" =        list("not yet implemented: cudf#3429" = "median v3 sd v3 by id4 id5", #121
                       "not yet implemented: cudf#2591" = "max v1 - min v2 by id3",
                       "not yet implemented: cudf#2592" = "largest two v3 by id6",
                       "not yet implemented: cudf#1267" = "regression v1 v2 by id2 id4"),
  "clickhouse" =  list()
)}
groupby.data.exceptions = {list(                                                             # exceptions as of run 1575727624
  "data.table" = {list(
    "timeout" = c("G1_1e9_1e1_0_0",                                                          # not always happened, q8 probably #110
                  "G1_1e9_2e0_0_0")                                                          # q4 #110 also sometimes segfaults during fread but not easily reproducible
  )},
  "dplyr" = {list(
    "out of memory" = c("G1_1e8_1e2_0_0","G1_1e8_1e2_0_1"),                                  # q10
    "timeout" = c("G1_1e7_1e2_0_0","G1_1e7_1e1_0_0","G1_1e7_2e0_0_0","G1_1e7_1e2_0_1",       # q10 # after going back to 0.8.3 all successfully finish
                  "G1_1e8_1e1_0_0","G1_1e8_2e0_0_0",                                         # q10
                  "G1_1e9_1e2_0_0","G1_1e9_1e2_0_1",                                         # q10
                  "G1_1e9_1e1_0_0",                                                          # q6
                  "G1_1e9_2e0_0_0")                                                          # q2 #110 also sometimes segfaults during fread but not easily reproducible
  )},
  "pandas" = {list(
    "out of memory" = c("G1_1e7_1e2_0_0","G1_1e7_1e1_0_0","G1_1e7_2e0_0_0","G1_1e7_1e2_0_1"),# q10 #142
    "out of memory" = c("G1_1e8_1e2_0_0","G1_1e8_1e1_0_0","G1_1e8_2e0_0_0","G1_1e8_1e2_0_1"),# q10 #142
    "out of memory" = c("G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1") # read_csv #99
  )},
  "pydatatable" = {list(
  )},
  "spark" = {list(
  )},
  "dask" = {list(
    "timeout" = c("G1_1e7_1e1_0_0",                                                # q8
                  "G1_1e7_2e0_0_0",                                                # q8
                  "G1_1e8_1e2_0_0",                                                # q8
                  "G1_1e8_1e1_0_0",                                                # q8
                  "G1_1e8_2e0_0_0",                                                # q8
                  "G1_1e8_1e2_0_1",                                                # q8
                  "G1_1e9_1e2_0_0",                                                # q3 #126
                  "G1_1e9_1e2_0_1",                                                # q3 #126
                  "G1_1e9_1e1_0_0",                                                # q3 #126
                  "G1_1e9_2e0_0_0")                                                # q3 #126
  )},
  "juliadf" = {list(
    "out of memory" = c("G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1") # CSV.File
  )},
  "cudf" = {list(
    "out of memory" = c("G1_1e8_1e2_0_0","G1_1e8_1e1_0_0","G1_1e8_2e0_0_0","G1_1e8_1e2_0_1", # read_csv #94
                        "G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1") # read_csv #97
  )},
  "clickhouse" = {list(
    "out of memory" = c("G1_1e9_1e2_0_0",                                                    # q10 #96
                        "G1_1e9_1e1_0_0",                                                    # q10 #96
                        "G1_1e9_2e0_0_0",                                                    # q10 #96
                        "G1_1e9_1e2_0_1")                                                    # q10 #96
  )}
)}
groupby.exceptions = task.exceptions(groupby.query.exceptions, groupby.data.exceptions)

# join ----

join_q_title_fun = function(x) {
  stopifnot(c("question","iquestion","out_rows","out_cols","in_rows") %in% names(x),
            uniqueN(x, by="iquestion")==nrow(x))
  x = copy(x)[, "iquestion":=rev(seq_along(iquestion))]
  x[, sprintf("Question %s: \"%s\": result %s x %s", iquestion, as.character(question), format_comma(out_rows), out_cols), by="iquestion"]$V1
}
join.syntax.dict = {list(
  "dask" = {c(
    "small inner on int" = "DF.merge(small, on='id1').compute()",
    "medium inner on int" = "DF.merge(medium, on='id2').compute()",
    "medium outer on int" = "DF.merge(medium, how='left', on='id2').compute()",
    "medium inner on factor" = "DF.merge(medium, on='id5').compute()",
    "big inner on int" = "DF.merge(big, on='id3').compute()"
  )},
  "data.table" = {c(
    "small inner on int" = "DT[small, on='id1', nomatch=NULL]",
    "medium inner on int" = "DT[medium, on='id2', nomatch=NULL]",
    "medium outer on int" = "medium[DT, on='id2']",
    "medium inner on factor" = "DT[medium, on='id5', nomatch=NULL]",
    "big inner on int" = "DT[big, on='id3', nomatch=NULL]"
  )},
  "dplyr" = {c(
    "small inner on int" = "inner_join(DF, small, by='id1')",
    "medium inner on int" = "inner_join(DF, medium, by='id2')",
    "medium outer on int" = "left_join(DF, medium, by='id2')",
    "medium inner on factor" = "inner_join(DF, medium, by='id5')",
    "big inner on int" = "inner_join(DF, big, by='id3')"
  )},
  "juliadf" = {c(
    "small inner on int" = "innerjoin(DF, small, on = :id1, makeunique=true)",
    "medium inner on int" = "innerjoin(DF, medium, on = :id2, makeunique=true)",
    "medium outer on int" = "leftjoin(DF, medium, on = :id2, makeunique=true)",
    "medium inner on factor" = "innerjoin(DF, medium, on = :id5, makeunique=true)",
    "big inner on int" = "innerjoin(DF, big, on = :id3, makeunique=true)"
  )},
  "pandas" = {c(
    "small inner on int" = "DF.merge(small, on='id1')",
    "medium inner on int" = "DF.merge(medium, on='id2')",
    "medium outer on int" = "DF.merge(medium, how='left', on='id2')",
    "medium inner on factor" = "DF.merge(medium, on='id5')",
    "big inner on int" = "DF.merge(big, on='id3')"
  )},
  "pydatatable" = {c(
    "small inner on int" = "y.key = 'id1'; DT[:, :, join(y)][isfinite(f.v2), :]",
    "medium inner on int" = "y.key = 'id2'; DT[:, :, join(y)][isfinite(f.v2), :]",
    "medium outer on int" = "y.key = 'id2'; DT[:, :, join(y)]",
    "medium inner on factor" = "y.key = 'id5'; DT[:, :, join(y)][isfinite(f.v2), :]",
    "big inner on int" = "y.key = 'id3'; DT[:, :, join(y)][isfinite(f.v2), :]"
  )},
  "spark" = {c(
    "small inner on int" = "spark.sql('select * from x join small using (id1)')",
    "medium inner on int" = "spark.sql('select * from x join medium using (id2)')",
    "medium outer on int" = "spark.sql('select * from x left join medium using (id2)')",
    "medium inner on factor" = "spark.sql('select * from x join medium using (id5)')",
    "big inner on int" = "spark.sql('select * from x join big using (id3)')"
  )},
  "clickhouse" = {c(
    "small inner on int" = "",
    "medium inner on int" = "",
    "medium outer on int" = "",
    "medium inner on factor" = "",
    "big inner on int" = ""
  )},
  "cudf" = {c(
    "small inner on int" = "DF.merge(small, on='id1')",
    "medium inner on int" = "DF.merge(medium, on='id2')",
    "medium outer on int" = "DF.merge(medium, how='left', on='id2')",
    "medium inner on factor" = "DF.merge(medium, on='id5')",
    "big inner on int" = "DF.merge(big, on='id3')"
  )}
)}
join.query.exceptions = {list(
  "data.table" =  list(),
  "dplyr" =       list(),
  "pandas" =      list(),
  "pydatatable" = list(),
  "spark" =       list(),
  "dask" =        list(),
  "juliadf" =     list(),
  "cudf" =        list(),
  "clickhouse" =  list()
)}
join.data.exceptions = {list(                                                             # exceptions as of run 1575727624
  "data.table" = {list(
    "out of memory" = c("J1_1e9_NA_0_0")                                                  # fread
  )},
  "dplyr" = {list(
    "out of memory" = c("J1_1e9_NA_0_0")                                                  # fread
  )},
  "pandas" = {list(
    "timeout" = c("J1_1e8_NA_0_0"),                                                       # q5 # now with extended timeout to 4h it finishes
    "out of memory" = c("J1_1e9_NA_0_0")                                                  # read_csv
  )},
  "pydatatable" = {list(
  )},
  "spark" = {list(
    "timeout" = c("J1_1e9_NA_0_0")                                                        # q5 using new 8h timeout #126
  )},
  "dask" = {list(
    "out of memory" = c("J1_1e8_NA_0_0"),                                                 # q5 using in-memory, after 93m (120m timeout)
    "out of memory" = c("J1_1e9_NA_0_0")                                                  # q1 even when using on-disk, after 47m (480m timeout)
  )},
  "juliadf" = {list(
    "timeout" = c("J1_1e8_NA_0_0"),                                                       # q3 not longer a problem after extending timeout, finishes in 93m (120m timeout)
    "out of memory" = c("J1_1e9_NA_0_0")                                                  # CSV.File
  )},
  "cudf" = {list(
    #"corrupted driver" = c("J1_1e7_NA_0_0","J1_1e8_NA_0_0"),                              # #129#issuecomment-573204532
    "out of memory" = c("J1_1e8_NA_0_0","J1_1e9_NA_0_0")                                   # read_csv #94 #97
  )},
  "clickhouse" = {list(
  )}
)}
join.exceptions = task.exceptions(join.query.exceptions, join.data.exceptions)
