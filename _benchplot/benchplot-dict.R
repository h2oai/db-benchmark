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
  tmp = strsplit(as.character(data_name), "_", fixed=TRUE)[[1L]]
  ds = data_spec(file, nrow=as.numeric(tmp[2]))
  na = as.numeric(tmp[4])
  sort = as.integer(tmp[5])
  extra = if (na>0 || sort>0L) {
    if (na>0 && sort>0L) sprintf(" %d%% NAs, pre-sorted", na)
    else if (na>0) sprintf(" %d%% NAs", na)
    else if (sort>0L) " pre-sorted"
    else stop("internal error")
  } else ""
  sprintf(
    "Input table: %s rows x %s columns ( %s GB )%s",
    format_comma(as.numeric(ds[["nrow"]])[1L]),
    as.numeric(ds[["ncol"]])[1L],
    as.numeric(ds[["gb"]])[1L],
    extra
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
  "cudf" = list(name=c(short="cuDF", long="cuDF"), color=c(strong="peachpuff3", light="peachpuff1")),
  "polars" = list(name=c(short="polars", long="Polars"), color=c(strong="deepskyblue4", light="deepskyblue3")),
  "arrow" = list(name=c(short="arrow", long="Arrow"), color=c(strong="aquamarine3", light="aquamarine1")),
  "duckdb" = list(name=c(short="duckdb", long="DuckDB"), color=c(strong="#ddcd07", light="#fff100"))
)}
#barplot(rep(c(0L,1L,1L), length(solution.dict)),
#        col=rev(c(rbind(sapply(solution.dict, `[[`, "color"), "black"))),
#        horiz=TRUE, axes=FALSE)

# groupby ----

groupby_q_title_fun = function(x) {
  stopifnot(c("question","iquestion","out_rows","out_cols","in_rows") %in% names(x),
            uniqueN(x, by="iquestion")==nrow(x))
  x = copy(x)[, "top2":=FALSE][, "iquestion":=rev(seq_along(iquestion))]
  x[question=="largest two v3 by id6", "top2":=TRUE] #118
  x[, sprintf("Query %s: \"%s\": %s%s ad hoc groups of ~%s rows;  result %s x %s",
              iquestion, as.character(question),
              if (top2) "~" else "",
              format_comma(if (top2) out_rows/2 else out_rows),
              if (top2) "2" else format_comma(as.numeric(as.character(in_rows))/as.numeric(out_rows)),
              format_comma(out_rows), out_cols),
    by = "iquestion"]$V1
}
groupby.syntax.dict = {list(
  "data.table" = {c(
    "sum v1 by id1" = "DT[, .(v1=sum(v1, na.rm=TRUE)), by=id1]",
    "sum v1 by id1:id2" = "DT[, .(v1=sum(v1, na.rm=TRUE)), by=.(id1, id2)]",
    "sum v1 mean v3 by id3" = "DT[, .(v1=sum(v1, na.rm=TRUE), v3=mean(v3, na.rm=TRUE)), by=id3]",
    "mean v1:v3 by id4" = "DT[, lapply(.SD, mean, na.rm=TRUE), by=id4, .SDcols=v1:v3]",
    "sum v1:v3 by id6" = "DT[, lapply(.SD, sum, na.rm=TRUE), by=id6, .SDcols=v1:v3]",
    "median v3 sd v3 by id4 id5" = "DT[, .(median_v3=median(v3, na.rm=TRUE), sd_v3=sd(v3, na.rm=TRUE)), by=.(id4, id5)]",
    "max v1 - min v2 by id3" = "DT[, .(range_v1_v2=max(v1, na.rm=TRUE)-min(v2, na.rm=TRUE)), by=id3]",
    "largest two v3 by id6" = "DT[order(-v3, na.last=NA), .(largest2_v3=head(v3, 2L)), by=id6]",
    "regression v1 v2 by id2 id4" = "DT[, .(r2=cor(v1, v2, use=\"na.or.complete\")^2), by=.(id2, id4)]",
    "sum v3 count by id1:id6" = "DT[, .(v3=sum(v3, na.rm=TRUE), count=.N), by=id1:id6]"
  )},
  "dplyr" = {c(
    "sum v1 by id1" = "DF %>% group_by(id1) %>% summarise(v1=sum(v1, na.rm=TRUE))",
    "sum v1 by id1:id2" = "DF %>% group_by(id1, id2) %>% summarise(v1=sum(v1, na.rm=TRUE))",
    "sum v1 mean v3 by id3" = "DF %>% group_by(id3) %>% summarise(v1=sum(v1, na.rm=TRUE), v3=mean(v3, na.rm=TRUE))",
    "mean v1:v3 by id4" = "DF %>% group_by(id4) %>% summarise_at(.funs=\"mean\", .vars=c(\"v1\",\"v2\",\"v3\"), na.rm=TRUE)",
    "sum v1:v3 by id6" = "DF %>% group_by(id6) %>% summarise_at(.funs=\"sum\", .vars=c(\"v1\",\"v2\",\"v3\"), na.rm=TRUE)",
    "median v3 sd v3 by id4 id5" = "DF %>% group_by(id4, id5) %>% summarise(median_v3=median(v3, na.rm=TRUE), sd_v3=sd(v3, na.rm=TRUE))",
    "max v1 - min v2 by id3" = "DF %>% group_by(id3) %>% summarise(range_v1_v2=max(v1, na.rm=TRUE)-min(v2, na.rm=TRUE))",
    "largest two v3 by id6" = "DF %>% select(id6, largest2_v3=v3) %>% filter(!is.na(largest2_v3)) %>% arrange(desc(largest2_v3)) %>% group_by(id6) %>% filter(row_number() <= 2L)",
    "regression v1 v2 by id2 id4" = "DF %>% group_by(id2, id4) %>% summarise(r2=cor(v1, v2, use=\"na.or.complete\")^2)",
    "sum v3 count by id1:id6" = "DF %>% group_by(id1, id2, id3, id4, id5, id6) %>% summarise(v3=sum(v3, na.rm=TRUE), count=n())"
  )},
  "pandas" = {c(
    "sum v1 by id1" = "DF.groupby('id1', as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'sum'})",
    "sum v1 by id1:id2" = "DF.groupby(['id1','id2'], as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'sum'})",
    "sum v1 mean v3 by id3" = "DF.groupby('id3', as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'sum', 'v3':'mean'})",
    "mean v1:v3 by id4" = "DF.groupby('id4', as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})",
    "sum v1:v3 by id6" = "DF.groupby('id6', as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})",
    "median v3 sd v3 by id4 id5" = "DF.groupby(['id4','id5'], as_index=False, sort=False, observed=True, dropna=False).agg({'v3': ['median','std']})",
    "max v1 - min v2 by id3" = "DF.groupby('id3', as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'max', 'v2':'min'}).assign(range_v1_v2=lambda x: x['v1']-x['v2'])[['id3','range_v1_v2']]",
    "largest two v3 by id6" = "DF[~DF['v3'].isna()][['id6','v3']].sort_values('v3', ascending=False).groupby('id6', as_index=False, sort=False, observed=True, dropna=False).head(2)",
    "regression v1 v2 by id2 id4" = "DF[['id2','id4','v1','v2']].groupby(['id2','id4'], as_index=False, sort=False, observed=True, dropna=False).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))",
    "sum v3 count by id1:id6" = "DF.groupby(['id1','id2','id3','id4','id5','id6'], as_index=False, sort=False, observed=True, dropna=False).agg({'v3':'sum', 'v1':'size'})"
  )},
  "pydatatable" = {c(
    "sum v1 by id1" = "DT[:, {'v1': sum(f.v1)}, by(f.id1)]",
    "sum v1 by id1:id2" = "DT[:, {'v1': sum(f.v1)}, by(f.id1, f.id2)]",
    "sum v1 mean v3 by id3" = "DT[:, {'v1': sum(f.v1), 'v3': mean(f.v3)}, by(f.id3)]",
    "mean v1:v3 by id4" = "DT[:, {'v1': mean(f.v1), 'v2': mean(f.v2), 'v3': mean(f.v3)}, by(f.id4)]",
    "sum v1:v3 by id6" = "DT[:, {'v1': sum(f.v1), 'v2': sum(f.v2), 'v3': sum(f.v3)}, by(f.id6)]",
    "median v3 sd v3 by id4 id5" = "DT[:, {'median_v3': median(f.v3), 'sd_v3': sd(f.v3)}, by(f.id4, f.id5)]",
    "max v1 - min v2 by id3" = "DT[:, {'range_v1_v2': max(f.v1)-min(f.v2)}, by(f.id3)]",
    "largest two v3 by id6" = "DT[~isna(f.v3),:][:2, {'largest2_v3': f.v3}, by(f.id6), sort(-f.v3)]",
    "regression v1 v2 by id2 id4" = "DT[:, {'r2': corr(f.v1, f.v2)**2}, by(f.id2, f.id4)]",
    "sum v3 count by id1:id6" = "DT[:, {'v3': sum(f.v3), 'count': count()}, by(f.id1, f.id2, f.id3, f.id4, f.id5, f.id6)]"
  )},
  "dask" = {c(
    "sum v1 by id1" = "DF.groupby('id1', dropna=False, observed=True).agg({'v1':'sum'}).compute()",
    "sum v1 by id1:id2" = "DF.groupby(['id1','id2'], dropna=False, observed=True).agg({'v1':'sum'}).compute()",
    "sum v1 mean v3 by id3" = "DF.groupby('id3', dropna=False, observed=True).agg({'v1':'sum', 'v3':'mean'}).compute()",
    "mean v1:v3 by id4" = "DF.groupby('id4', dropna=False, observed=True).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()",
    "sum v1:v3 by id6" = "DF.groupby('id6', dropna=False, observed=True).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()",
    "median v3 sd v3 by id4 id5" = "", #  DF.groupby(['id4','id5'], dropna=False, observed=True).agg({'v3': ['median','std']}).compute()"
    "max v1 - min v2 by id3" = "DF.groupby('id3', dropna=False, observed=True).agg({'v1':'max', 'v2':'min'}).assign(range_v1_v2=lambda x: x['v1']-x['v2'])[['range_v1_v2']].compute()",
    "largest two v3 by id6" = "DF[~DF['v3'].isna()][['id6','v3']].groupby('id6', dropna=False, observed=True).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id6':'Int64', 'v3':'float64'})[['v3']].compute()",
    "regression v1 v2 by id2 id4" = "DF[['id2','id4','v1','v2']].groupby(['id2','id4'], dropna=False, observed=True).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}), meta={'r2':'float64'}).compute()",
    "sum v3 count by id1:id6" = "DF.groupby(['id1','id2','id3','id4','id5','id6'], dropna=False, observed=True).agg({'v3':'sum', 'v1':'size'}).compute()"
  )},
  "spark" = {c(
    "sum v1 by id1" = "SELECT id1, sum(v1) AS v1 FROM tbl GROUP BY id1",
    "sum v1 by id1:id2" = "SELECT id1, id2, sum(v1) AS v1 FROM tbl GROUP BY id1, id2",
    "sum v1 mean v3 by id3" = "SELECT id3, sum(v1) AS v1, mean(v3) AS v3 FROM tbl GROUP BY id3",
    "mean v1:v3 by id4" = "SELECT id4, mean(v1) AS v1, mean(v2) AS v2, mean(v3) AS v3 FROM tbl GROUP BY id4",
    "sum v1:v3 by id6" = "SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM tbl GROUP BY id6",
    "median v3 sd v3 by id4 id5" = "", # "SELECT id4, id5, median(v3) AS median_v3, stddev(v3) AS sd_v3 FROM tbl GROUP BY id4, id5"
    "max v1 - min v2 by id3" = "SELECT id3, max(v1)-min(v2) AS range_v1_v2 FROM tbl GROUP BY id3",
    "largest two v3 by id6" = "SELECT id6, largest2_v3 from (SELECT id6, v3 AS largest2_v3, row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS order_v3 FROM tbl WHERE v3 IS NOT NULL) sub_query WHERE order_v3 <= 2",
    "regression v1 v2 by id2 id4" = "SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM tbl GROUP BY id2, id4",
    "sum v3 count by id1:id6" = "SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count(*) AS count FROM tbl GROUP BY id1, id2, id3, id4, id5, id6"
  )},
  "juliadf" = {c(
    "sum v1 by id1" = "combine(groupby(DF, :id1), :v1 => sum∘skipmissing => :v1)",
    "sum v1 by id1:id2" = "combine(groupby(DF, [:id1, :id2]), :v1 => sum∘skipmissing => :v1)",
    "sum v1 mean v3 by id3" = "combine(groupby(DF, :id3), :v1 => sum∘skipmissing => :v1, :v3 => mean∘skipmissing => :v3)",
    "mean v1:v3 by id4" = "combine(groupby(DF, :id4), :v1 => mean∘skipmissing => :v1, :v2 => mean∘skipmissing => :v2, :v3 => mean∘skipmissing => :v3)",
    "sum v1:v3 by id6" = "combine(groupby(DF, :id6), :v1 => sum∘skipmissing => :v1, :v2 => sum∘skipmissing => :v2, :v3 => sum∘skipmissing => :v3)",
    "median v3 sd v3 by id4 id5" = "combine(groupby(DF, [:id4, :id5]), :v3 => median∘skipmissing => :median_v3, :v3 => std∘skipmissing => :sd_v3)",
    "max v1 - min v2 by id3" = "combine(groupby(DF, :id3), [:v1, :v2] => ((v1, v2) -> maximum(skipmissing(v1))-minimum(skipmissing(v2))) => :range_v1_v2)",
    "largest two v3 by id6" = "combine(groupby(dropmissing(DF, :v3), :id6), :v3 => (x -> partialsort!(x, 1:min(2, length(x)), rev=true)) => :largest2_v3)",
    "regression v1 v2 by id2 id4" = "combine(groupby(DF, [:id2, :id4]), [:v1, :v2] => ((v1,v2) -> cor(v1, v2)^2) => :r2)",
    "sum v3 count by id1:id6" = "combine(groupby(DF, [:id1, :id2, :id3, :id4, :id5, :id6]), :v3 => sum∘skipmissing => :v3, :v3 => length => :count)"
  )},
  "cudf" = {c(
    "sum v1 by id1" = "DF.groupby('id1', as_index=False, dropna=False).agg({'v1':'sum'}).compute()",
    "sum v1 by id1:id2" = "DF.groupby(['id1','id2'], as_index=False, dropna=False).agg({'v1':'sum'}).compute()",
    "sum v1 mean v3 by id3" = "DF.groupby('id3', as_index=False, dropna=False).agg({'v1':'sum', 'v3':'mean'}).compute()",
    "mean v1:v3 by id4" = "DF.groupby('id4', as_index=False, dropna=False).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()",
    "sum v1:v3 by id6" = "DF.groupby('id6', as_index=False, dropna=False).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()",
    "median v3 sd v3 by id4 id5" = "DF.groupby(['id4','id5'], as_index=False, dropna=False).agg({'v3': ['median','std']}).compute()",
    "max v1 - min v2 by id3" = "", # "DF.groupby('id3', as_index=False, dropna=False).agg({'v1':'max', 'v2':'min'}).assign(range_v1_v2=lambda x: x['v1']-x['v2'])[['range_v1_v2']].compute()"
    "largest two v3 by id6" = "", # "DF[~x['v3'].isna()][['id6','v3']].sort_values('v3', ascending=False).groupby('id6', as_index=False, dropna=False).head(2).compute()"
    "regression v1 v2 by id2 id4" = "", # "DF[['id2','id4','v1','v2']].groupby(['id2','id4'], as_index=False, dropna=False).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2})).compute()"
    "sum v3 count by id1:id6" = "DF.groupby(['id1','id2','id3','id4','id5','id6'], as_index=False, dropna=False).agg({'v3':'sum', 'v1':'size'}).compute()"
  )},
  "clickhouse" = {c(
    "sum v1 by id1" = "SELECT id1, sum(v1) AS v1 FROM tbl GROUP BY id1",
    "sum v1 by id1:id2" = "SELECT id1, id2, sum(v1) AS v1 FROM tbl GROUP BY id1, id2",
    "sum v1 mean v3 by id3" = "SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM tbl GROUP BY id3",
    "mean v1:v3 by id4" = "SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM tbl GROUP BY id4",
    "sum v1:v3 by id6" = "SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM tbl GROUP BY id6",
    "median v3 sd v3 by id4 id5" = "SELECT id4, id5, medianExact(v3) AS median_v3, stddevPop(v3) AS sd_v3 FROM tbl GROUP BY id4, id5",
    "max v1 - min v2 by id3" = "SELECT id3, max(v1) - min(v2) AS range_v1_v2 FROM tbl GROUP BY id3",
    "largest two v3 by id6" = "SELECT id6, arrayJoin(arraySlice(arrayReverseSort(groupArray(v3)), 1, 2)) AS v3 FROM (SELECT id6, v3 FROM tbl WHERE v3 IS NOT NULL) AS subq GROUP BY id6",
    "regression v1 v2 by id2 id4" = "SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM tbl GROUP BY id2, id4",
    "sum v3 count by id1:id6" = "SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count() AS cnt FROM tbl GROUP BY id1, id2, id3, id4, id5, id6"
  )},
  "polars" = {c(
    "sum v1 by id1" = "DF.groupby('id1').agg(pl.sum('v1')).collect()",
    "sum v1 by id1:id2" = "DF.groupby(['id1','id2']).agg(pl.sum('v1')).collect()",
    "sum v1 mean v3 by id3" = "DF.groupby('id3').agg([pl.sum('v1'), pl.mean('v3')]).collect()",
    "mean v1:v3 by id4" = "DF.groupby('id4').agg([pl.mean('v1'), pl.mean('v2'), pl.mean('v3')]).collect()",
    "sum v1:v3 by id6" = "DF.groupby('id6').agg([pl.sum('v1'), pl.sum('v2''), pl.sum('v3'')]).collect()",
    "median v3 sd v3 by id4 id5" = "DF.groupby(['id4','id5']).agg([pl.median('v3').alias('v3_median'), pl.std('v3').alias('v3_std')]).collect()",
    "max v1 - min v2 by id3" = "DF.groupby('id3').agg([(pl.max('v1') - pl.min('v2')).alias('range_v1_v2')]).collect()",
    "largest two v3 by id6" = "DF.drop_nulls('v3').sort('v3', reverse=True).groupby('id6').agg(col('v3').head(2).alias('largest2_v3')).explode('largest2_v3').collect()",
    "regression v1 v2 by id2 id4" = "DF.groupby(['id2','id4']).agg((pl.pearson_corr('v1','v2')**2).alias('r2')).collect()",
    "sum v3 count by id1:id6" = "DF.groupby(['id1','id2','id3','id4','id5','id6']).agg([pl.sum('v3').alias('v3'), pl.count('v1').alias('count')]).collect()"
  )},
  "arrow" = {c(
    "sum v1 by id1" = "AT %>% group_by(id1) %>% summarise(v1=sum(v1, na.rm=TRUE))",
    "sum v1 by id1:id2" = "AT %>% group_by(id1, id2) %>% summarise(v1=sum(v1, na.rm=TRUE))",
    "sum v1 mean v3 by id3" = "AT %>% group_by(id3) %>% summarise(v1=sum(v1, na.rm=TRUE), v3=mean(v3, na.rm=TRUE))",
    "mean v1:v3 by id4" = "AT %>% group_by(id4) %>% summarise_at(.funs=\"mean\", .vars=c(\"v1\",\"v2\",\"v3\"), na.rm=TRUE)",
    "sum v1:v3 by id6" = "AT %>% group_by(id6) %>% summarise_at(.funs=\"sum\", .vars=c(\"v1\",\"v2\",\"v3\"), na.rm=TRUE)",
    "median v3 sd v3 by id4 id5" = "AT %>% group_by(id4, id5) %>% summarise(median_v3=median(v3, na.rm=TRUE), sd_v3=sd(v3, na.rm=TRUE))",
    "max v1 - min v2 by id3" = "AT %>% group_by(id3) %>% summarise(range_v1_v2=max(v1, na.rm=TRUE)-min(v2, na.rm=TRUE))",
    "largest two v3 by id6" = "AT %>% select(id6, largest2_v3=v3) %>% filter(!is.na(largest2_v3)) %>% arrange(desc(largest2_v3)) %>% group_by(id6) %>% filter(row_number() <= 2L)",
    "regression v1 v2 by id2 id4" = "AT %>% group_by(id2, id4) %>% summarise(r2=cor(v1, v2, use=\"na.or.complete\")^2)",
    "sum v3 count by id1:id6" = "AT %>% group_by(id1, id2, id3, id4, id5, id6) %>% summarise(v3=sum(v3, na.rm=TRUE), count=n())"
  )},
  "duckdb" = {c(
    "sum v1 by id1" = "SELECT id1, sum(v1) AS v1 FROM tbl GROUP BY id1",
    "sum v1 by id1:id2" = "SELECT id1, id2, sum(v1) AS v1 FROM tbl GROUP BY id1, id2",
    "sum v1 mean v3 by id3" = "SELECT id3, sum(v1) AS v1, mean(v3) AS v3 FROM tbl GROUP BY id3",
    "mean v1:v3 by id4" = "SELECT id4, mean(v1) AS v1, mean(v2) AS v2, mean(v3) AS v3 FROM tbl GROUP BY id4",
    "sum v1:v3 by id6" = "SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM tbl GROUP BY id6",
    "median v3 sd v3 by id4 id5" = "SELECT id4, id5, quantile_cont(v3, 0.5) AS median_v3, stddev(v3) AS sd_v3 FROM tbl GROUP BY id4, id5",
    "max v1 - min v2 by id3" = "SELECT id3, max(v1)-min(v2) AS range_v1_v2 FROM tbl GROUP BY id3",
    "largest two v3 by id6" = "SELECT id6, v3 AS largest2_v3 FROM (SELECT id6, v3, row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS order_v3 FROM x WHERE v3 IS NOT NULL) sub_query WHERE order_v3 <= 2",
    "regression v1 v2 by id2 id4" = "SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM tbl GROUP BY id2, id4",
    "sum v3 count by id1:id6" = "SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count(*) AS count FROM tbl GROUP BY id1, id2, id3, id4, id5, id6"
  )}
)}
groupby.query.exceptions = {list(
  "data.table" =  list(),
  "dplyr" =       list(),
  "pandas" =      list(),
  "pydatatable" = list(),
  "spark" =       list("not yet implemented: SPARK-26589" = "median v3 sd v3 by id4 id5"),
  "dask" =        list("not yet implemented: dask#4362" = "median v3 sd v3 by id4 id5"),
  "juliadf" =     list(),
  "cudf" =        list("not yet implemented: cudf#2591" = "max v1 - min v2 by id3",
                       "not yet implemented: cudf#2592" = "largest two v3 by id6",
                       "not yet implemented: cudf#1267" = "regression v1 v2 by id2 id4"),
  "clickhouse" =  list(),
  "polars"     =  list(),
  "arrow"      =  list(),
  "duckdb"     =  list("inaccurate: duckdb#1738" = "median v3 sd v3 by id4 id5")
)}
groupby.data.exceptions = {list(                                                             # exceptions as of run 1575727624
  "data.table" = {list(
    "timeout" = c("G1_1e9_1e1_0_0",                                                          # not always happened, q8 probably #110
                  "G1_1e9_2e0_0_0")                                                          # q4 #110 also sometimes segfaults during fread but not easily reproducible
  )},
  "dplyr" = {list(
    "timeout" = c("G1_1e8_2e0_0_0"),                                                         # q10
    "internal error" = c("G1_1e9_1e2_0_0","G1_1e9_1e2_0_1","G1_1e9_1e2_5_0",                 # q1 #152
                  "G1_1e9_1e1_0_0",                                                          # q2 #152, before was q6
                  "G1_1e9_2e0_0_0")                                                          # q3 #152, before was q2 #110 also sometimes segfaults during fread but not easily reproducible
  )},
  "pandas" = {list(
    "not yet implemented: pandas#36327" = c("G1_1e7_1e2_5_0","G1_1e8_1e2_5_0","G1_1e9_1e2_5_0"), # #171
    "out of memory" = c("G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1") # read_csv #99
  )},
  "pydatatable" = {list(
    "csv reader NAs bug: datatable#2808" = c("G1_1e9_1e2_5_0")
  )},
  "spark" = {list(
    "timeout" = "G1_1e9_1e2_5_0" ## seems that both runs have finished but second run timing was not logged to time.csv due to timeout
  )},
  "dask" = {list(
    "not yet implemented: dask#6986" = c("G1_1e7_1e2_5_0","G1_1e8_1e2_5_0","G1_1e9_1e2_5_0"), # #171
    "internal error" = "G1_1e8_1e2_0_0",                                           # q10 #174
    "out of memory" = c("G1_1e7_1e2_0_0","G1_1e7_1e2_0_1",                         # q10 #176
                        "G1_1e8_1e2_0_1",                                          # q10
                        "G1_1e9_1e2_0_0","G1_1e9_1e2_0_1","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0"), # read.csv
    "timeout" = c("G1_1e7_1e1_0_0",                                                # q10
                  "G1_1e7_2e0_0_0",                                                # q10
                  "G1_1e8_1e1_0_0",                                                # q7
                  "G1_1e8_2e0_0_0")                                                # q3
  )},
  "juliadf" = {list(
    "timeout" = "G1_1e8_2e0_0_0",
    "out of memory" = c("G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1","G1_1e9_1e2_5_0") # CSV.File
  )},
  "cudf" = {list(
    "out of memory" = c("G1_1e8_1e2_0_0","G1_1e8_1e1_0_0","G1_1e8_2e0_0_0","G1_1e8_1e2_0_1","G1_1e8_1e2_5_0", # read_csv #94
                        "G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1","G1_1e9_1e2_5_0") # read_csv #97
  )},
  "clickhouse" = {list(
  )},
  "polars" = {list(
    "out of memory" = c("G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1","G1_1e9_1e2_5_0") # q10
  )},
  "arrow" = {list(
    "timeout" = "G1_1e8_2e0_0_0", # q10
    "internal error" = c("G1_1e9_1e2_0_0","G1_1e9_1e2_0_1","G1_1e9_1e2_5_0","G1_1e9_1e1_0_0", # inherits from dplyr
                         "G1_1e9_2e0_0_0") # #190
  )},
  "duckdb" = {list(
    "out of memory" = c("G1_1e9_1e2_0_0","G1_1e9_1e1_0_0","G1_1e9_2e0_0_0","G1_1e9_1e2_0_1","G1_1e9_1e2_5_0"),
    "incorrect: duckdb#1737" = c("G1_1e7_1e2_5_0","G1_1e8_1e2_5_0")
  )}
)}
groupby.exceptions = task.exceptions(groupby.query.exceptions, groupby.data.exceptions)

# join ----

join_q_title_fun = function(x) {
  stopifnot(c("question","iquestion","out_rows","out_cols","in_rows") %in% names(x),
            uniqueN(x, by="iquestion")==nrow(x))
  x = copy(x)[, "iquestion":=rev(seq_along(iquestion))]
  x[, sprintf("Query %s: \"%s\": result %s x %s", iquestion, as.character(question), format_comma(out_rows), out_cols), by="iquestion"]$V1
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
    "small inner on int" = "innerjoin(DF, small, on = :id1, makeunique=true, matchmissing=:equal)",
    "medium inner on int" = "innerjoin(DF, medium, on = :id2, makeunique=true, matchmissing=:equal)",
    "medium outer on int" = "leftjoin(DF, medium, on = :id2, makeunique=true, matchmissing=:equal)",
    "medium inner on factor" = "innerjoin(DF, medium, on = :id5, makeunique=true, matchmissing=:equal)",
    "big inner on int" = "innerjoin(DF, big, on = :id3, makeunique=true, matchmissing=:equal)"
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
    "small inner on int" = "select * from x join small using (id1)",
    "medium inner on int" = "select * from x join medium using (id2)",
    "medium outer on int" = "select * from x left join medium using (id2)",
    "medium inner on factor" = "select * from x join medium using (id5)",
    "big inner on int" = "select * from x join big using (id3)"
  )},
  "clickhouse" = {c(
    "small inner on int" = "SELECT id1, x.id2, x.id3, x.id4, y.id4, x.id5, x.id6, x.v1, y.v2 FROM x INNER JOIN y USING (id1)",
    "medium inner on int" = "SELECT x.id1, y.id1, id2, x.id3, x.id4, y.id4, x.id5, y.id5, x.id6, x.v1, y.v2 FROM x INNER JOIN y USING (id2)",
    "medium outer on int" = "SELECT x.id1, y.id1, id2, x.id3, x.id4, y.id4, x.id5, y.id5, x.id6, x.v1, y.v2 FROM x LEFT JOIN y USING (id2)",
    "medium inner on factor" = "SELECT x.id1, y.id1, x.id2, y.id2, x.id3, x.id4, y.id4, id5, x.id6, x.v1, y.v2 FROM x INNER JOIN y USING (id5)",
    "big inner on int" = "SELECT x.id1, y.id1, x.id2, y.id2, id3, x.id4, y.id4, x.id5, y.id5, x.id6, y.id6, x.v1, y.v2 FROM x INNER JOIN y USING (id3)"
  )},
  "cudf" = {c(
    "small inner on int" = "DF.merge(small, on='id1').compute()",
    "medium inner on int" = "DF.merge(medium, on='id2').compute()",
    "medium outer on int" = "DF.merge(medium, how='left', on='id2').compute()",
    "medium inner on factor" = "DF.merge(medium, on='id5').compute()",
    "big inner on int" = "DF.merge(big, on='id3').compute()"
  )},
  "polars" = {c(
    "small inner on int" = "DF.merge(small, on='id1')",
    "medium inner on int" = "DF.merge(medium, on='id2')",
    "medium outer on int" = "DF.merge(medium, how='left', on='id2')",
    "medium inner on factor" = "DF.merge(medium, on='id5')",
    "big inner on int" = "DF.merge(big, on='id3')"
  )},
  "arrow" = {c(
    "small inner on int" = "inner_join(DF, small, by='id1')",
    "medium inner on int" = "inner_join(DF, medium, by='id2')",
    "medium outer on int" = "left_join(DF, medium, by='id2')",
    "medium inner on factor" = "inner_join(DF, medium, by='id5')",
    "big inner on int" = "inner_join(DF, big, by='id3')"
  )},
  "duckdb" = {c(
    "small inner on int" = "SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1)",
    "medium inner on int" = "SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x JOIN medium USING (id2)",
    "medium outer on int" = "SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x LEFT JOIN medium USING (id2)",
    "medium inner on factor" = "SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 FROM x JOIN medium USING (id5)",
    "big inner on int" = "SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x JOIN big USING (id3)"
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
  "clickhouse" =  list(),
  "polars"     =  list(),
  "arrow"      =  list(),
  "duckdb"     =  list()
)}
join.data.exceptions = {list(                                                             # exceptions as of run 1575727624
  "data.table" = {list(
    "out of memory" = c("J1_1e9_NA_0_0","J1_1e9_NA_5_0","J1_1e9_NA_0_1")                  # fread
  )},
  "dplyr" = {list(
    "out of memory" = c("J1_1e9_NA_0_0","J1_1e9_NA_5_0","J1_1e9_NA_0_1")                  # fread
  )},
  "pandas" = {list(
    "out of memory" = c("J1_1e9_NA_0_0","J1_1e9_NA_5_0","J1_1e9_NA_0_1")                  # read_csv
  )},
  "pydatatable" = {list(
    "csv reader NAs bug: datatable#2808" = "J1_1e9_NA_5_0",
    "out of memory" = c("J1_1e9_NA_0_0","J1_1e9_NA_0_1")                                  # q5 out of memory due to a deep copy
  )},
  "spark" = {list(
    "timeout" = c("J1_1e9_NA_0_0","J1_1e9_NA_5_0","J1_1e9_NA_0_1")                        # q5 using new 8h timeout #126
  )},
  "dask" = {list(
    "internal error: dask#7015" = c("J1_1e7_NA_0_0","J1_1e7_NA_5_0","J1_1e7_NA_0_1",      # dask/dask#7015
                         "J1_1e8_NA_0_0","J1_1e8_NA_5_0","J1_1e8_NA_0_1",
                         "J1_1e9_NA_5_0","J1_1e9_NA_0_1"),
    "out of memory" = c("J1_1e9_NA_0_0")                                                  # q1 even when using on-disk, after 47m (480m timeout)
  )},
  "juliadf" = {list(
    "out of memory" = c("J1_1e9_NA_0_0","J1_1e9_NA_5_0","J1_1e9_NA_0_1")                  # CSV.File
  )},
  "cudf" = {list(
    "out of memory" = c("J1_1e8_NA_0_0","J1_1e8_NA_5_0","J1_1e8_NA_0_1",                  # read_csv #94 #97
                        "J1_1e9_NA_0_0","J1_1e9_NA_5_0","J1_1e9_NA_0_1")
  )},
  "clickhouse" = {list(
    "out of memory" = c("J1_1e9_NA_0_0",                                                  # q1 r2 #169
                        "J1_1e9_NA_5_0","J1_1e9_NA_0_1")                                  # q1 r1
  )},
  "polars" = {list(
    "out of memory" = c("J1_1e9_NA_0_0","J1_1e9_NA_5_0","J1_1e9_NA_0_1")
  )},
  "arrow" = {list(
    "not yet implemented: #189" = c("J1_1e7_NA_0_0","J1_1e7_NA_5_0","J1_1e7_NA_0_1","J1_1e8_NA_0_0","J1_1e8_NA_5_0","J1_1e8_NA_0_1","J1_1e9_NA_0_0","J1_1e9_NA_5_0","J1_1e9_NA_0_1")
  )},
  "duckdb" = {list(
    "internal error: duckdb#1739" = c("J1_1e7_NA_0_0","J1_1e7_NA_5_0","J1_1e7_NA_0_1","J1_1e8_NA_0_0","J1_1e8_NA_5_0","J1_1e8_NA_0_1"),
    "out of memory" = c("J1_1e9_NA_0_0","J1_1e9_NA_5_0","J1_1e9_NA_0_1")#,
    #"incorrect: duckdb#1737" = c("J1_1e7_NA_5_0","J1_1e8_NA_5_0")
  )}
)}
join.exceptions = task.exceptions(join.query.exceptions, join.data.exceptions)

# groupby2014 ----

groupby2014.syntax.dict = {list(
  "data.table" = {c(
    "sum v1 by id1" = "DT[, sum(v1), keyby=id1]",
    "sum v1 by id1:id2" = "DT[, sum(v1), keyby='id1,id2']",
    "sum v1 mean v3 by id3" = "DT[, list(sum(v1), mean(v3)), keyby=id3]",
    "mean v1:v3 by id4" = "DT[, lapply(.SD, mean), keyby=id4, .SDcols=7:9]",
    "sum v1:v3 by id6" = "DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9]"
  )},
  "dplyr" = {c(
    "sum v1 by id1" = "DF %>% group_by(id1) %>% summarise(sum(v1))",
    "sum v1 by id1:id2" = "DF %>% group_by(id1,id2) %>% summarise(sum(v1))",
    "sum v1 mean v3 by id3" = "DF %>% group_by(id3) %>% summarise(sum(v1), mean(v3))",
    "mean v1:v3 by id4" = "DF %>% group_by(id4) %>% summarise(across(v1:v3, mean))",
    "sum v1:v3 by id6" = "DF %>% group_by(id6) %>% summarise(across(v1:v3, sum))"
  )},
  "pandas" = {c(
    "sum v1 by id1" = "DF.groupby(['id1']).agg({'v1':'sum'})",
    "sum v1 by id1:id2" = "DF.groupby(['id1','id2']).agg({'v1':'sum'})",
    "sum v1 mean v3 by id3" = "DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})",
    "mean v1:v3 by id4" = "DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})",
    "sum v1:v3 by id6" = "DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})"
  )}
)}
groupby2014.query.exceptions = {list(
  "data.table" =  list(),
  "dplyr" =       list(),
  "pandas" =      list()
)}
groupby2014.data.exceptions = {list(
  "data.table" = {list(
  )},
  "dplyr" = {list(
    "internal error" = "G0_1e9_1e2_0_0"
  )},
  "pandas" = {list(
    "out of memory" = "G0_1e9_1e2_0_0"
  )}
)}
groupby2014.exceptions = task.exceptions(groupby2014.query.exceptions, groupby2014.data.exceptions)
