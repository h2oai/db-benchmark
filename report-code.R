
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
  "median v3 sd v3 by id2 id4" = {c(
    "dask" = "", # x.groupby(['id2','id4']).agg({'v3': ['median','std']}).compute()
    "data.table" = "DT[, .(median_v3=median(v3), sd_v3=sd(v3)), by=.(id2, id4)]",
    "dplyr" = "DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(median_v3=median(v3), sd_v3=sd(v3))",
    "juliadf" = "by(x, [:id2, :id4], median_v3 = :v3 => median, sd_v3 = :v3 => std)",
    "pandas" = "x.groupby(['id2','id4']).agg({'v3': ['median','std']})",
    "pydatatable" = "", # x[:, {'median_v3': median(f.v3), 'sd_v3': sd(f.v3)}, by(f.id2, f.id4)]
    "spark" = "", # spark.sql('select id2, id4, median(v3) as median_v3, stddev(v3) as sd_v3 from x group by id2, id4')
    "clickhouse" = "SELECT id2, id4, medianExact(v3) AS median_v3, stddevPop(v3) AS sd_v3 FROM x GROUP BY id2, id4",
    "cudf" = ""
  )}, # q6
  "max v1 - min v2 by id2 id4" = {c(
    "dask" = "x.groupby(['id2','id4']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']].compute()",
    "data.table" = "DT[, .(range_v1_v2=max(v1)-min(v2)), by=.(id2, id4)]",
    "dplyr" = "DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(range_v1_v2=max(v1)-min(v2))",
    "juliadf" = "by(x, [:id2, :id4], range_v1_v2 = [:v1, :v2] => x -> maximum(skipmissing(x.v1))-minimum(skipmissing(x.v2)))",
    "pandas" = "x.groupby(['id2','id4']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']]",
    "pydatatable" = "x[:, {'range_v1_v2': max(f.v1)-min(f.v2)}, by(f.id2, f.id4)]",
    "spark" = "spark.sql('select id2, id4, max(v1)-min(v2) as range_v1_v2 from x group by id2, id4')",
    "clickhouse" ="SELECT id2, id4, max(v1) - min(v2) AS range_v1_v2 FROM x GROUP BY id2, id4",
    "cudf" = ""
  )}, # q7
  "largest two v3 by id2 id4" = {c(
    "dask" = "x[['id2','id4','v3']].groupby(['id2','id4']).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id2': 'category', 'id4': 'int64', 'v3': 'float64'})[['v3']].compute()",
    "data.table" = "DT[order(-v3), .(largest2_v3=head(v3, 2L)), by=.(id2, id4)]",
    "dplyr" = "DF %>% select(id2, id4, largest2_v3=v3) %>% arrange(desc(largest2_v3)) %>% group_by(id2, id4, .drop=TRUE) %>% filter(row_number() <= 2L)",
    "juliadf" = "by(x, [:id2, :id4], largest2_v3 = :v3 => x -> partialsort(x, 1:2, rev=true))",
    "pandas" = "x[['id2','id4','v3']].sort_values('v3', ascending=False).groupby(['id2','id4']).head(2)",
    "pydatatable" = "x[:2, {'largest2_v3': f.v3}, by(f.id2, f.id4), sort(-f.v3)]",
    "spark" = "spark.sql('select id2, id4, largest2_v3 from (select id2, id4, v3 as largest2_v3, row_number() over (partition by id2, id4 order by v3 desc) as order_v3 from x) sub_query where order_v3 <= 2')",
    "clickhouse" = "SELECT id2, id4, arrayJoin(arraySlice(arrayReverseSort(groupArray(v3)), 1, 2)) AS v3 FROM x GROUP BY id2, id4",
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

groupby.data.exceptions = {list(                                                             # exceptions as of run 1566398304
  "data.table" = {list(
    "timeout" = c("G1_1e9_2e0_0_0")                                                          # q3
  )},
  "dplyr" = {list(
    "timeout" = c("G1_1e7_1e2_0_0","G1_1e7_1e1_0_0","G1_1e7_2e0_0_0","G1_1e7_1e2_0_1",       # q10
                  "G1_1e8_1e2_0_0","G1_1e8_1e1_0_0","G1_1e8_2e0_0_0","G1_1e8_1e2_0_1",       # q10
                  "G1_1e9_1e2_0_0","G1_1e9_1e2_0_1",                                         # q10
                  "G1_1e9_1e1_0_0",                                                          # q6
                  "G1_1e9_2e0_0_0")                                                          # q3
  )},
  "pandas" = {list(
    "MemoryError" = c("G1_1e7_1e2_0_0","G1_1e7_1e1_0_0","G1_1e7_2e0_0_0","G1_1e7_1e2_0_1"),  # q10
    "timeout" = c("G1_1e8_1e2_0_0","G1_1e8_1e1_0_0","G1_1e8_2e0_0_0","G1_1e8_1e2_0_1"),      # q10
    "MemoryError on read CSV" = c("G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1") # read_csv #99
  )},
  "pydatatable" = {list(
  )},
  "spark" = {list(
  )},
  "dask" = {list(
    "Segmentation fault" = "G1_1e9_1e1_0_0",                                                 # read_csv
    "MemoryError on read CSV" = c("G1_1e9_1e2_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1")        # read_csv    #99
  )},
  "juliadf" = {list(
    "out of memory" = c("G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1") # CSV.File
  )},
  "cudf" = {list(
    "print ans fatal error" = "G1_1e7_2e0_0_0",                                               # q2         #102
    "out of memory" = c("G1_1e8_1e2_0_0","G1_1e8_1e1_0_0","G1_1e8_2e0_0_0","G1_1e8_1e2_0_1", # read_csv    #94
                        "G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1") # read_csv    #97
  )},
  "clickhouse" = {list(
    "Memory limit" = c("G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_1e2_0_1"),                  # q10         #96
    "CH server crash" = "G1_1e9_2e0_0_0"                                                     # q3
  )}
)}
groupby.query.exceptions = {list(
  "data.table" =  list(),
  "dplyr" =       list(),
  "pandas" =      list(),
  "pydatatable" = list("not yet implemented: datatable#1530" = "median v3 sd v3 by id2 id4",
                       "not yet implemented: datatable#1543" = "regression v1 v2 by id2 id4"),
  "spark" =       list("not yet implemented: SPARK-26589" = "median v3 sd v3 by id2 id4"),
  "dask" =        list("not yet implemented: dask#4362" = "median v3 sd v3 by id2 id4",
                       "not yet implemented: dask#4828" = "regression v1 v2 by id2 id4"),
  "juliadf" =     list(),
  "cudf" =        list("not yet implemented: cudf#1085" = "median v3 sd v3 by id2 id4",
                       "not yet implemented: cudf#2591" = "max v1 - min v2 by id2 id4",
                       "not yet implemented: cudf#2592" = "largest two v3 by id2 id4",
                       "not yet implemented: cudf#1267" = "regression v1 v2 by id2 id4"),
  "clickhouse" =  list()
)}
groupby.exceptions = list(query = groupby.query.exceptions, data = groupby.data.exceptions)
stopifnot(
  sapply(groupby.exceptions$query, function(x) {y<-unlist(x, use.names=FALSE);length(unique(y))==length(y)}),
  sapply(groupby.exceptions$data, function(x) {y<-unlist(x, use.names=FALSE);length(unique(y))==length(y)})
)

# join ----

join.code = list(
  "small inner on int" = c( # q1
    "dask" = "x.merge(small, on='id4').compute()",
    "data.table" = "DT[small, on=.(id4), nomatch=NULL]",
    "dplyr" = "inner_join(DF, small, by='id4')",
    "juliadf" = "",
    "pandas" = "x.merge(small, on='id4')",
    "pydatatable" = "x[:, :, join(small, on='id4)]",
    "spark" = "spark.sql('select * from x join small on x.id4 = small.id4').persist(pyspark.StorageLevel.MEMORY_ONLY)",
    "clickhouse"="",
    "cudf"=""
  ),
  "medium inner on int" = c( # q2
    "dask" = "x.merge(medium, on='id4').compute()",
    "data.table" = "DT[medium, on=.(id4), nomatch=NULL]",
    "dplyr" = "inner_join(DF, medium, by='id4')",
    "juliadf" = "",
    "pandas" = "x.merge(medium, on='id4')",
    "pydatatable" = "x[:, :, join(medium, on='id4')]",
    "spark" = "spark.sql('select * from x join medium on x.id4 = medium.id4').persist(pyspark.StorageLevel.MEMORY_ONLY)",
    "clickhouse"="",
    "cudf"=""
  ),
  "medium outer on int" = c( # q3
    "dask" = "x.merge(medium, how='left', on='id4').compute()",
    "data.table" = "DT[medium, on=.(id4)]",
    "dplyr" = "left_join(DF, medium, by='id4')",
    "juliadf" = "",
    "pandas" = "x.merge(medium, how='left', on='id4')",
    "pydatatable" = "x[:, :, join(medium, on='id4', how='outer')]",
    "spark" = "spark.sql('select * from x left join medium on x.id4 = medium.id4').persist(pyspark.StorageLevel.MEMORY_ONLY)",
    "clickhouse"="",
    "cudf"=""
  ),
  "medium inner on factor" = c( # q4
    "dask" = "x.merge(medium, on='id1').compute()",
    "data.table" = "DT[medium, on=.(id1), nomatch=NULL]",
    "dplyr" = "inner_join(DF, medium, by='id1')",
    "juliadf" = "",
    "pandas" = "x.merge(medium, on='id1')",
    "pydatatable" = "x[:, :, join(medium, on='id1')]",
    "spark" = "spark.sql('select * from x join medium on x.id1 = medium.id1').persist(pyspark.StorageLevel.MEMORY_ONLY)",
    "clickhouse"="",
    "cudf"=""
  ),
  "big inner on int" = c( # q5
    "dask" = "x.merge(big, on='id4').compute()",
    "data.table" = "DT[big, on=.(id4), nomatch=NULL]",
    "dplyr" = "inner_join(DF, big, by='id4')",
    "juliadf" = "",
    "pandas" = "x.merge(big, on='id4')",
    "pydatatable" = "x[:, :, join(big, on='id4')]",
    "spark" = "spark.sql('select * from x join big on x.id4 = big.id4').persist(pyspark.StorageLevel.MEMORY_ONLY)",
    "clickhouse"="",
    "cudf"=""
  )
)

# template ----

#"" = c( # q0
#  "dask" = "",
#  "data.table" = "",
#  "dplyr" = "",
#  "juliadf" = "",
#  "pandas" = "",
#  "pydatatable" = "",
#  "spark" = ""
#  "clickhouse" = "",
#  "cudf" = ""
#)
