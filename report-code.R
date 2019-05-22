groupby.code = list(
  "sum v1 by id1" = c( # q1
    "dask"="x.groupby(['id1']).agg({'v1':'sum'}).compute()",
    "data.table"="DT[, .(v1=sum(v1)), by=id1]",
    "dplyr"="DF %>% group_by(id1, .drop=TRUE) %>% summarise(sum(v1))",
    "juliadf"="by(x, :id1, v1 = :v1 => sum)",
    "pandas"="DF.groupby(['id1']).agg({'v1':'sum'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1)}, by(f.id1)]",
    "spark"="spark.sql('select id1, sum(v1) as v1 from x group by id1')"
  ),
  "sum v1 by id1:id2" = c( # q2
    "dask"="x.groupby(['id1','id2']).agg({'v1':'sum'}).compute()",
    "data.table"="DT[, .(v1=sum(v1)), by=.(id1, id2)]",
    "dplyr"="DF %>% group_by(id1, id2, .drop=TRUE) %>% summarise(sum(v1))",
    "juliadf"="by(x, [:id1, :id2], v1 = :v1 => sum)",
    "pandas"="DF.groupby(['id1','id2']).agg({'v1':'sum'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1)}, by(f.id1, f.id2)]",
    "spark"="spark.sql('select id1, id2, sum(v1) as v1 from x group by id1, id2')"
  ),
  "sum v1 mean v3 by id3" = c( # q3
    "dask"="x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'}).compute()",
    "data.table"="DT[, .(v1=sum(v1), v3=mean(v3)), by=id3]",
    "dplyr"="DF %>% group_by(id3, .drop=TRUE) %>% summarise(sum(v1), mean(v3))",
    "juliadf"="by(x, :id3, v1 = :v1 => sum, v3 = :v3 => mean)",
    "pandas"="DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1), 'v3': mean(f.v3)}, by(f.id3)]",
    "spark"="spark.sql('select id3, sum(v1) as v1, mean(v3) as v3 from x group by id3')"
  ),
  "mean v1:v3 by id4" = c( # q4
    "dask"="x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()",
    "data.table"="DT[, lapply(.SD, mean), by=id4, .SDcols=v1:v3]",
    "dplyr"="DF %>% group_by(id4, .drop=TRUE) %>% summarise_each(funs(mean), vars=7:9)",
    "juliadf"="by(x, :id4, v1 = :v1 => mean, v2 = :v2 => mean, v3 = :v3 => mean)",
    "pandas"="DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})",
    "pydatatable"="DT[:, {'v1': mean(f.v1), 'v2': mean(f.v2), 'v3': mean(f.v3)}, by(f.id4)]",
    "spark"="spark.sql('select id4, mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from x group by id4')"
  ),
  "sum v1:v3 by id6" = c( # q5
    "dask"="x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()",
    "data.table"="DT[, lapply(.SD, sum), by=id6, .SDcols=v1:v3]",
    "dplyr"="DF %>% group_by(id6, .drop=TRUE) %>% summarise_each(funs(sum), vars=7:9)",
    "juliadf"="by(x, :id6, v1 = :v1 => sum, v2 = :v2 => sum, v3 = :v3 => sum)",
    "pandas"="DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1), 'v2': sum(f.v2), 'v3': sum(f.v3)}, by(f.id6)]",
    "spark"="spark.sql('select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6')"
  ),
  "median v3 sd v3 by id2 id4" = c ( # q6
    "dask" = "median not yet implemented: dask#4362", # x.groupby(['id2','id4']).agg({'v3': ['median','std']}).compute()
    "data.table" = "DT[, .(median_v3=median(v3), sd_v3=sd(v3)), by=.(id2, id4)]",
    "dplyr" = "DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(median_v3=median(v3), sd_v3=sd(v3))",
    "juliadf" = "by(x, [:id2, :id4], median_v3 = :v3 => median, sd_v3 = :v3 => std)",
    "pandas" = "x.groupby(['id2','id4']).agg({'v3': ['median','std']})",
    "pydatatable" = "median not yet implemented: datatable#1530", # x[:, {'median_v3': median(f.v3), 'sd_v3': sd(f.v3)}, by(f.id2, f.id4)]
    "spark" = "median not yet implemented: SPARK-26589" # spark.sql('select id2, id4, median(v3) as median_v3, stddev(v3) as sd_v3 from x group by id2, id4')
  ),
  "max v1 - min v2 by id2 id4" = c ( # q7
    "dask" = "x.groupby(['id2','id4']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']].compute()",
    "data.table" = "DT[, .(range_v1_v2=max(v1)-min(v2)), by=.(id2, id4)]",
    "dplyr" = "DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(range_v1_v2=max(v1)-min(v2))",
    "juliadf" = "by(x, [:id2, :id4], range_v1_v2 = [:v1, :v2] => x -> maximum(skipmissing(x.v1))-minimum(skipmissing(x.v2)))",
    "pandas" = "x.groupby(['id2','id4']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']]",
    "pydatatable" = "x[:, {'range_v1_v2': max(f.v1)-min(f.v2)}, by(f.id2, f.id4)]",
    "spark" = "spark.sql('select id2, id4, max(v1)-min(v2) as range_v1_v2 from x group by id2, id4')"
  ),
  "largest two v3 by id2 id4" = c ( # q8
    "dask" = "x[['id2','id4','v3']].groupby(['id2','id4']).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id2': 'category', 'id4': 'int64', 'v3': 'float64'})[['v3']].compute()",
    "data.table" = "DT[order(-v3), .(largest2_v3=head(v3, 2L)), by=.(id2, id4)]",
    "dplyr" = "DF %>% select(id2, id4, largest2_v3=v3) %>% arrange(desc(largest2_v3)) %>% group_by(id2, id4, .drop=TRUE) %>% filter(row_number() <= 2L)",
    "juliadf" = "by(x, [:id2, :id4], largest2_v3 = :v3 => x -> partialsort(x, 1:2, rev=true))",
    "pandas" = "x[['id2','id4','v3']].sort_values('v3', ascending=False).groupby(['id2','id4']).head(2)",
    "pydatatable" = "x[:2, {'largest2_v3': f.v3}, by(f.id2, f.id4), sort(-f.v3)]",
    "spark" = "spark.sql('select id2, id4, largest2_v3 from (select id2, id4, v3 as largest2_v3, row_number() over (partition by id2, id4 order by v3 desc) as order_v3 from x) sub_query where order_v3 <= 2')"
  ),
  "regression v1 v2 by id2 id4" = c ( # q9
    "dask" = "not yet implemented: dask/dask#4828",
    "data.table" = "DT[, .(r2=cor(v1, v2)^2), by=.(id2, id4)]",
    "dplyr" = "DF %>% group_by(id2, id4, .drop=TRUE) %>% summarise(r2=cor(v1, v2)^2)",
    "juliadf" = "by(x, [:id2, :id4], r2 = [:v1, :v2] => x -> cor(x.v1, x.v2)^2)",
    "pandas" = "x[['id2','id4','v1','v2']].groupby(['id2','id4']).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))",
    "pydatatable" = "not yet implemented: datatable#1543", # x[:, {'r2': cor(v1, v2)^2}, by(f.id2, f.id4)],
    "spark" = "spark.sql('select id2, id4, pow(corr(v1, v2), 2) as r2 from x group by id2, id4')"
  ),
  "sum v3 count by id1:id6" = c( # q10
    "dask" = "x.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'}).compute()",
    "data.table" = "DT[, .(v3=sum(v3), count=.N), by=id1:id6]",
    "dplyr" = "DF %>% group_by(id1, id2, id3, id4, id5, id6, .drop=TRUE) %>% summarise(v3=sum(v3), count=n())",
    "juliadf" = "by(x, [:id1, :id2, :id3, :id4, :id5, :id6], v3 = :v3 => sum, count = :v3 => length)",
    "pandas" = "x.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'})",
    "pydatatable" = "x[:, {'v3': sum(f.v3), 'count': count()}, by(f.id1, f.id2, f.id3, f.id4, f.id5, f.id6)]",
    "spark" = "spark.sql('select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6')"
  )
)

join.code = list(
  "small inner on int" = c( # q1
    "dask" = "x.merge(small, on='id4').compute()",
    "data.table" = "DT[small, on=.(id4), nomatch=NULL]",
    "dplyr" = "inner_join(DF, small, by='id4')",
    "juliadf" = "",
    "pandas" = "x.merge(small, on='id4')",
    "pydatatable" = "x[:, :, join(small, on='id4)]",
    "spark" = "spark.sql('select * from x join small on x.id4 = small.id4').persist(pyspark.StorageLevel.MEMORY_ONLY)"
  ),
  "medium inner on int" = c( # q2
    "dask" = "x.merge(medium, on='id4').compute()",
    "data.table" = "DT[medium, on=.(id4), nomatch=NULL]",
    "dplyr" = "inner_join(DF, medium, by='id4')",
    "juliadf" = "",
    "pandas" = "x.merge(medium, on='id4')",
    "pydatatable" = "x[:, :, join(medium, on='id4')]",
    "spark" = "spark.sql('select * from x join medium on x.id4 = medium.id4').persist(pyspark.StorageLevel.MEMORY_ONLY)"
  ),
  "medium outer on int" = c( # q3
    "dask" = "x.merge(medium, how='left', on='id4').compute()",
    "data.table" = "DT[medium, on=.(id4)]",
    "dplyr" = "left_join(DF, medium, by='id4')",
    "juliadf" = "",
    "pandas" = "x.merge(medium, how='left', on='id4')",
    "pydatatable" = "x[:, :, join(medium, on='id4', how='outer')]",
    "spark" = "spark.sql('select * from x left join medium on x.id4 = medium.id4').persist(pyspark.StorageLevel.MEMORY_ONLY)"
  ),
  "medium inner on factor" = c( # q4
    "dask" = "x.merge(medium, on='id1').compute()",
    "data.table" = "DT[medium, on=.(id1), nomatch=NULL]",
    "dplyr" = "inner_join(DF, medium, by='id1')",
    "juliadf" = "",
    "pandas" = "x.merge(medium, on='id1')",
    "pydatatable" = "x[:, :, join(medium, on='id1')]",
    "spark" = "spark.sql('select * from x join medium on x.id1 = medium.id1').persist(pyspark.StorageLevel.MEMORY_ONLY)"
  ),
  "big inner on int" = c( # q5
    "dask" = "x.merge(big, on='id4').compute()",
    "data.table" = "DT[big, on=.(id4), nomatch=NULL]",
    "dplyr" = "inner_join(DF, big, by='id4')",
    "juliadf" = "",
    "pandas" = "x.merge(big, on='id4')",
    "pydatatable" = "x[:, :, join(big, on='id4')]",
    "spark" = "spark.sql('select * from x join big on x.id4 = big.id4').persist(pyspark.StorageLevel.MEMORY_ONLY)"
  )
)

#"" = c( # q0
#  "dask" = "",
#  "data.table" = "",
#  "dplyr" = "",
#  "juliadf" = "",
#  "pandas" = "",
#  "pydatatable" = "",
#  "spark" = ""
#)
