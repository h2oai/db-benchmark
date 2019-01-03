groupby.code = list(
  "sum v1 by id1" = c(
    "dask"="x.groupby(['id1']).agg({'v1':'sum'}).compute()",
    "data.table"="DT[, .(v1=sum(v1)), by=id1]",
    "dplyr"="DF %>% group_by(id1) %>% summarise(sum(v1))",
    "juliadf"="by(x, :id1, v1 = :v1=>sum)",
    "pandas"="DF.groupby(['id1']).agg({'v1':'sum'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1)}, by(f.id1)]",
    "spark"="spark.sql('select sum(v1) as v1 from x group by id1')"
  ),
  "sum v1 by id1:id2" = c(
    "dask"="x.groupby(['id1','id2']).agg({'v1':'sum'}).compute()",
    "data.table"="DT[, .(v1=sum(v1)), by=.(id1, id2)]",
    "dplyr"="DF %>% group_by(id1,id2) %>% summarise(sum(v1))",
    "juliadf"="by(x, [:id1, :id2], v1 = :v1=>sum)",
    "pandas"="DF.groupby(['id1','id2']).agg({'v1':'sum'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1)}, by(f.id1, f.id2)]",
    "spark"="spark.sql('select sum(v1) as v1 from x group by id1, id2')"
  ),
  "sum v1 mean v3 by id3" = c(
    "dask"="x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'}).compute()",
    "data.table"="DT[, .(v1=sum(v1), v3=mean(v3)), by=id3]",
    "dplyr"="DF %>% group_by(id3) %>% summarise(sum(v1), mean(v3))",
    "juliadf"="by(x, :id3, v1 = :v1=>sum, v3 = :v3=>mean)",
    "pandas"="DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1), 'v3': mean(f.v3)}, by(f.id3)]",
    "spark"="spark.sql('select sum(v1) as v1, mean(v3) as v3 from x group by id3')"
  ),
  "mean v1:v3 by id4" = c(
    "dask"="x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()",
    "data.table"="DT[, lapply(.SD, mean), by=id4, .SDcols=v1:v3]",
    "dplyr"="DF %>% group_by(id4) %>% summarise_each(funs(mean), vars=7:9)",
    "juliadf"="by(x, :id4, v1 = :v1=>mean, v2 = :v2=>mean, v3 = :v3=>mean)",
    "pandas"="DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})",
    "pydatatable"="DT[:, {'v1': mean(f.v1), 'v2': mean(f.v2), 'v3': mean(f.v3)}, by(f.id4)]",
    "spark"="spark.sql('select mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from x group by id4')"
  ),
  "sum v1:v3 by id6" = c(
    "dask"="x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()",
    "data.table"="DT[, lapply(.SD, sum), by=id6, .SDcols=v1:v3]",
    "dplyr"="DF %>% group_by(id6) %>% summarise_each(funs(sum), vars=7:9)",
    "juliadf"="by(x, :id6, v1 = :v1=>sum, v2 = :v2=>sum, v3 = :v3=>sum)",
    "pandas"="DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})",
    "pydatatable"="DT[:, {'v1': sum(f.v1), 'v2': sum(f.v2), 'v3': sum(f.v3)}, by(f.id6)]",
    "spark"="spark.sql('select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6')"
  )
)
