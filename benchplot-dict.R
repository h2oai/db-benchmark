
# exceptions helper ----

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

# groupby ----

groupby.code = list(
  "sum v1 by id1" = {c(
    "dask"="x.groupby(['id1']).agg({'v1':'sum'}).compute()",
    "data.table"="DT[, .(v1=sum(v1)), by=id1]",
    "dplyr"="DF %>% group_by(id1, .drop=TRUE) %>% summarise(sum(v1))",
    "juliadf"="by(x, :id1, v1 = :v1 => sum)",
    "pandas"="DF.groupby(['id1']).agg({'v1':'sum'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1)}, by(f.id1)]",
    "spark"="spark.sql('select id1, sum(v1) as v1 from x group by id1')",
    "clickhouse"="SELECT id1, sum(v1) AS v1 FROM x GROUP BY id1",
    "cudf"="x.groupby(['id1'],as_index=False).agg({'v1':'sum'})"
  )}, # q1
  "sum v1 by id1:id2" = {c(
    "dask"="x.groupby(['id1','id2']).agg({'v1':'sum'}).compute()",
    "data.table"="DT[, .(v1=sum(v1)), by=.(id1, id2)]",
    "dplyr"="DF %>% group_by(id1, id2, .drop=TRUE) %>% summarise(sum(v1))",
    "juliadf"="by(x, [:id1, :id2], v1 = :v1 => sum)",
    "pandas"="DF.groupby(['id1','id2']).agg({'v1':'sum'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1)}, by(f.id1, f.id2)]",
    "spark"="spark.sql('select id1, id2, sum(v1) as v1 from x group by id1, id2')",
    "clickhouse"="SELECT id1, id2, sum(v1) AS v1 FROM x GROUP BY id1, id2",
    "cudf"="x.groupby(['id1','id2'],as_index=False).agg({'v1':'sum'})"
  )}, # q2
  "sum v1 mean v3 by id3" = {c(
    "dask"="x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'}).compute()",
    "data.table"="DT[, .(v1=sum(v1), v3=mean(v3)), by=id3]",
    "dplyr"="DF %>% group_by(id3, .drop=TRUE) %>% summarise(sum(v1), mean(v3))",
    "juliadf"="by(x, :id3, v1 = :v1 => sum, v3 = :v3 => mean)",
    "pandas"="DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1), 'v3': mean(f.v3)}, by(f.id3)]",
    "spark"="spark.sql('select id3, sum(v1) as v1, mean(v3) as v3 from x group by id3')",
    "clickhouse"="SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM x GROUP BY id3;",
    "cudf"="x.groupby(['id3'],as_index=False).agg({'v1':'sum', 'v3':'mean'})"
  )}, # q3
  "mean v1:v3 by id4" = {c(
    "dask"="x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()",
    "data.table"="DT[, lapply(.SD, mean), by=id4, .SDcols=v1:v3]",
    "dplyr"="DF %>% group_by(id4, .drop=TRUE) %>% summarise_each(funs(mean), vars=7:9)",
    "juliadf"="by(x, :id4, v1 = :v1 => mean, v2 = :v2 => mean, v3 = :v3 => mean)",
    "pandas"="DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})",
    "pydatatable"="DT[:, {'v1': mean(f.v1), 'v2': mean(f.v2), 'v3': mean(f.v3)}, by(f.id4)]",
    "spark"="spark.sql('select id4, mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from x group by id4')",
    "clickhouse"="SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM x GROUP BY id4",
    "cudf"="x.groupby(['id4'],as_index=False).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})"
  )}, # q4
  "sum v1:v3 by id6" = {c(
    "dask"="x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()",
    "data.table"="DT[, lapply(.SD, sum), by=id6, .SDcols=v1:v3]",
    "dplyr"="DF %>% group_by(id6, .drop=TRUE) %>% summarise_each(funs(sum), vars=7:9)",
    "juliadf"="by(x, :id6, v1 = :v1 => sum, v2 = :v2 => sum, v3 = :v3 => sum)",
    "pandas"="DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1), 'v2': sum(f.v2), 'v3': sum(f.v3)}, by(f.id6)]",
    "spark"="spark.sql('select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6')",
    "clickhouse"="SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM x GROUP BY id6",
    "cudf"="x.groupby(['id6'],as_index=False).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})"
  )}, # q5
  "median v3 sd v3 by id4 id5" = {c(
    "dask" = "", # x.groupby(['id4','id5']).agg({'v3': ['median','std']}).compute()
    "data.table" = "DT[, .(median_v3=median(v3), sd_v3=sd(v3)), by=.(id4, id5)]",
    "dplyr" = "DF %>% group_by(id4, id5, .drop=TRUE) %>% summarise(median_v3=median(v3), sd_v3=sd(v3))",
    "juliadf" = "by(x, [:id4, :id5], median_v3 = :v3 => median, sd_v3 = :v3 => std)",
    "pandas" = "x.groupby(['id4','id5']).agg({'v3': ['median','std']})",
    "pydatatable" = "", # x[:, {'median_v3': median(f.v3), 'sd_v3': sd(f.v3)}, by(f.id4, f.id5)]
    "spark" = "", # spark.sql('select id4, id5, median(v3) as median_v3, stddev(v3) as sd_v3 from x group by id4, id5')
    "clickhouse" = "SELECT id4, id5, medianExact(v3) AS median_v3, stddevPop(v3) AS sd_v3 FROM x GROUP BY id4, id5",
    "cudf" = ""
  )}, # q6
  "max v1 - min v2 by id3" = {c(
    "dask" = "x.groupby(['id3']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']].compute()",
    "data.table" = "DT[, .(range_v1_v2=max(v1)-min(v2)), by=id3]",
    "dplyr" = "DF %>% group_by(id3, .drop=TRUE) %>% summarise(range_v1_v2=max(v1)-min(v2))",
    "juliadf" = "by(x, [:id3], range_v1_v2 = [:v1, :v2] => x -> maximum(skipmissing(x.v1))-minimum(skipmissing(x.v2)))",
    "pandas" = "x.groupby(['id3']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']]",
    "pydatatable" = "x[:, {'range_v1_v2': max(f.v1)-min(f.v2)}, by(f.id3)]",
    "spark" = "spark.sql('select id3, max(v1)-min(v2) as range_v1_v2 from x group by id3')",
    "clickhouse" ="SELECT id3, max(v1) - min(v2) AS range_v1_v2 FROM x GROUP BY id3",
    "cudf" = ""
  )}, # q7
  "largest two v3 by id6" = {c(
    "dask" = "x[['id6','v3']].groupby(['id6']).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id6': 'int64', 'v3': 'float64'})[['v3']].compute()",
    "data.table" = "DT[order(-v3), .(largest2_v3=head(v3, 2L)), by=id6]",
    "dplyr" = "DF %>% select(id6, largest2_v3=v3) %>% arrange(desc(largest2_v3)) %>% group_by(id6, .drop=TRUE) %>% filter(row_number() <= 2L)",
    "juliadf" = "by(x, [:id6], largest2_v3 = :v3 => x -> partialsort(x, 1:2, rev=true))",
    "pandas" = "x[['id6','v3']].sort_values('v3', ascending=False).groupby(['id6']).head(2)",
    "pydatatable" = "x[:2, {'largest2_v3': f.v3}, by(f.id6), sort(-f.v3)]",
    "spark" = "spark.sql('select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from x) sub_query where order_v3 <= 2')",
    "clickhouse" = "SELECT id6, arrayJoin(arraySlice(arrayReverseSort(groupArray(v3)), 1, 2)) AS v3 FROM x GROUP BY id6",
    "cudf" = ""
  )}, # q8
  "regression v1 v2 by id2 id4" = {c(
    "dask" = "",
    "data.table" = "DT[, .(r2=cor(v1, v2)^2), by=.(id2, id4)]",
    "dplyr" = "DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(r2=cor(v1, v2)^2)",
    "juliadf" = "by(x, [:id2, :id4], r2 = [:v1, :v2] => x -> cor(x.v1, x.v2)^2)",
    "pandas" = "x[['id2','id4','v1','v2']].groupby(['id2','id4']).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))",
    "pydatatable" = "", # x[:, {'r2': cor(v1, v2)^2}, by(f.id2, f.id4)],
    "spark" = "spark.sql('select id2, id4, pow(corr(v1, v2), 2) as r2 from x group by id2, id4')",
    "clickhouse" = "SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4",
    "cudf" = ""
  )}, # q9
  "sum v3 count by id1:id6" = {c(
    "dask" = "x.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'}).compute()",
    "data.table" = "DT[, .(v3=sum(v3), count=.N), by=id1:id6]",
    "dplyr" = "DF %>% group_by(id1, id2, id3, id4, id5, id6, .drop=TRUE) %>% summarise(v3=sum(v3), count=n())",
    "juliadf" = "by(x, [:id1, :id2, :id3, :id4, :id5, :id6], v3 = :v3 => sum, count = :v3 => length)",
    "pandas" = "x.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'})",
    "pydatatable" = "x[:, {'v3': sum(f.v3), 'count': count()}, by(f.id1, f.id2, f.id3, f.id4, f.id5, f.id6)]",
    "spark" = "spark.sql('select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6')",
    "clickhouse" = "SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count() AS cnt FROM x GROUP BY id1, id2, id3, id4, id5, id6",
    "cudf" = "x.groupby(['id1','id2','id3','id4','id5','id6'],as_index=False).agg({'v3':'sum', 'v1':'count'})"
  )} # q10
)

groupby.query.exceptions = {list(
  "data.table" =  list(),
  "dplyr" =       list(),
  "pandas" =      list(),
  "pydatatable" = list("not yet implemented: datatable#1530" = "median v3 sd v3 by id4 id5",
                       "not yet implemented: datatable#1543" = "regression v1 v2 by id2 id4"),
  "spark" =       list("not yet implemented: SPARK-26589" = "median v3 sd v3 by id4 id5"),
  "dask" =        list("not yet implemented: dask#4362" = "median v3 sd v3 by id4 id5",
                       "not yet implemented: dask#4828" = "regression v1 v2 by id2 id4"),
  "juliadf" =     list(),
  "cudf" =        list("not yet implemented: cudf#1085" = "median v3 sd v3 by id4 id5",
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
    "improper syntax" = c("G1_1e7_1e1_0_0","G1_1e7_2e0_0_0",
                          "G1_1e8_1e1_0_0","G1_1e8_2e0_0_0"),                      # q8 #117#issuecomment-555314119
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

# join ----

join.code = list(
  "small inner on int" = {c(
    "dask" = "x.merge(small, on='id1').compute()",
    "data.table" = "DT[small, on='id1', nomatch=NULL]",
    "dplyr" = "inner_join(DF, small, by='id1')",
    "juliadf" = "join(x, small, on = :id1, makeunique=true)",
    "pandas" = "x.merge(small, on='id1')",
    "pydatatable" = "y.key = 'id1'; x[:, :, join(y)][isfinite(f.v2), :]",
    "spark" = "spark.sql('select * from x join small using (id1)')",
    "clickhouse" = "",
    "cudf" = "x.merge(small, on='id1')"
  )}, # q1
  "medium inner on int" = {c(
    "dask" = "x.merge(medium, on='id2').compute()",
    "data.table" = "DT[medium, on='id2', nomatch=NULL]",
    "dplyr" = "inner_join(DF, medium, by='id2')",
    "juliadf" = "join(x, medium, on = :id2, makeunique=true)",
    "pandas" = "x.merge(medium, on='id2')",
    "pydatatable" = "y.key = 'id2'; x[:, :, join(y)][isfinite(f.v2), :]",
    "spark" = "spark.sql('select * from x join medium using (id2)')",
    "clickhouse" = "",
    "cudf" = "x.merge(medium, on='id2')"
  )}, # q2
  "medium outer on int" = {c(
    "dask" = "x.merge(medium, how='left', on='id2').compute()",
    "data.table" = "medium[DT, on='id2']",
    "dplyr" = "left_join(DF, medium, by='id2')",
    "juliadf" = "join(x, medium, kind = :left, on = :id2, makeunique=true)",
    "pandas" = "x.merge(medium, how='left', on='id2')",
    "pydatatable" = "y.key = 'id2'; x[:, :, join(y)]",
    "spark" = "spark.sql('select * from x left join medium using (id2)')",
    "clickhouse" = "",
    "cudf" = "x.merge(medium, how='left', on='id2')"
  )}, # q3
  "medium inner on factor" = {c(
    "dask" = "x.merge(medium, on='id5').compute()",
    "data.table" = "DT[medium, on='id5', nomatch=NULL]",
    "dplyr" = "inner_join(DF, medium, by='id5')",
    "juliadf" = "join(x, medium, on = :id5, makeunique=true)",
    "pandas" = "x.merge(medium, on='id5')",
    "pydatatable" = "y.key = 'id5'; x[:, :, join(y)][isfinite(f.v2), :]",
    "spark" = "spark.sql('select * from x join medium using (id5)')",
    "clickhouse" = "",
    "cudf" = "x.merge(medium, on='id5')"
  )}, # q4
  "big inner on int" = {c(
    "dask" = "x.merge(big, on='id3').compute()",
    "data.table" = "DT[big, on='id3', nomatch=NULL]",
    "dplyr" = "inner_join(DF, big, by='id3')",
    "juliadf" = "join(x, big, on = :id3, makeunique=true)",
    "pandas" = "x.merge(big, on='id3')",
    "pydatatable" = "y.key = 'id3'; x[:, :, join(y)][isfinite(f.v2), :]",
    "spark" = "spark.sql('select * from x join big using (id3)')",
    "clickhouse" = "",
    "cudf" = "x.merge(big, on='id3')"
  )} # q5
)

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
join.data.exceptions = {list(                                                             # exceptions as of run 1572448371
  "data.table" = {list(
    "out of memory" = c("J1_1e9_NA_0_0")                                                  # fread
  )},
  "dplyr" = {list(
    "out of memory" = c("J1_1e9_NA_0_0")                                                  # fread
  )},
  "pandas" = {list(
    "timeout" = c("J1_1e8_NA_0_0"),                                                       # q5
    "out of memory" = c("J1_1e9_NA_0_0")                                                  # read_csv
  )},
  "pydatatable" = {list(
  )},
  "spark" = {list(
    "out of memory" = c("J1_1e9_NA_0_0")                                                  # read_csv
  )},
  "dask" = {list(
    "timeout" = c("J1_1e8_NA_0_0"),                                                       # q4
    "timeout" = c("J1_1e9_NA_0_0")                                                        # read_csv
  )},
  "juliadf" = {list(
    "timeout" = c("J1_1e8_NA_0_0"),                                                       # q3
    "timeout" = c("J1_1e9_NA_0_0")                                                        # CSV.File
  )},
  "cudf" = {list(
    "out of memory" = c("J1_1e8_NA_0_0","J1_1e9_NA_0_0")                                  # read_csv
  )},
  "clickhouse" = {list()}
)}
join.exceptions = task.exceptions(join.query.exceptions, join.data.exceptions)

# task template ----

.task.code = list(
  "question1" = {c(
    "dask" = "",
    "data.table" = "",
    "dplyr" = "",
    "juliadf" = "",
    "pandas" = "",
    "pydatatable" = "",
    "spark" = "",
    "clickhouse" = "",
    "cudf" = ""
  )}, # q1
  "question2" = {c(
    "dask" = "",
    "data.table" = "",
    "dplyr" = "",
    "juliadf" = "",
    "pandas" = "",
    "pydatatable" = "",
    "spark" = "",
    "clickhouse" = "",
    "cudf" = ""
  )}, # q2
  "question3" = {c(
    "dask" = "",
    "data.table" = "",
    "dplyr" = "",
    "juliadf" = "",
    "pandas" = "",
    "pydatatable" = "",
    "spark" = "",
    "clickhouse" = "",
    "cudf" = ""
  )}, # q3
  "question4" = {c(
    "dask" = "",
    "data.table" = "",
    "dplyr" = "",
    "juliadf" = "",
    "pandas" = "",
    "pydatatable" = "",
    "spark" = "",
    "clickhouse" = "",
    "cudf" = ""
  )}, # q4
  "question5" = {c(
    "dask" = "",
    "data.table" = "",
    "dplyr" = "",
    "juliadf" = "",
    "pandas" = "",
    "pydatatable" = "",
    "spark" = "",
    "clickhouse" = "",
    "cudf" = ""
  )} # q5
)

.task.query.exceptions = {list(
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
.task.data.exceptions = {list(                                                             # exceptions as of run ?
  "data.table" = {list()},
  "dplyr" = {list()},
  "pandas" = {list()},
  "pydatatable" = {list()},
  "spark" = {list()},
  "dask" = {list()},
  "juliadf" = {list()},
  "cudf" = {list()},
  "clickhouse" = {list()}
)}
.task.exceptions = task.exceptions(.task.query.exceptions, .task.data.exceptions)
