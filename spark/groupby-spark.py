#!/usr/bin/env python3

print("# groupby-spark.py", flush=True)

import os
import gc
import timeit
import pyspark
from pyspark.sql import SparkSession

exec(open("./_helpers/helpers.py").read())

ver = pyspark.__version__
git = "" # won't fix: https://issues.apache.org/jira/browse/SPARK-16864
task = "groupby"
solution = "spark"
fun = ".sql"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ['SRC_DATANAME']
src_grp = os.path.join("data", data_name+".csv")
print("loading dataset %s" % data_name, flush=True)

mem_usage = "100g"
if "TEST_RUN" in os.environ:
     mem_usage = "2g"

from pyspark.conf import SparkConf
spark = SparkSession.builder \
     .master("local[*]") \
     .appName("groupby-spark") \
     .config("spark.executor.memory", mem_usage) \
     .config("spark.driver.memory", mem_usage) \
     .config("spark.python.worker.memory", mem_usage) \
     .config("spark.driver.maxResultSize", mem_usage) \
     .config("spark.network.timeout", "2400") \
     .config("spark.executor.heartbeatInterval", "1200") \
     .config("spark.ui.showConsoleProgress", "false") \
     .getOrCreate()
#print(spark.sparkContext._conf.getAll(), flush=True)

x = spark.read.csv(src_grp, header=True, inferSchema='true').persist(pyspark.StorageLevel.MEMORY_ONLY)

print(x.count(), flush=True)

x.createOrReplaceTempView("x")

task_init = timeit.default_timer()
print("grouping...", flush=True)

question = "sum v1 by id1" # q1
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id1, sum(v1) as v1 from x group by id1").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id1, sum(v1) as v1 from x group by id1").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "sum v1 by id1:id2" # q2
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id1, id2, sum(v1) as v1 from x group by id1, id2").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id1, id2, sum(v1) as v1 from x group by id1, id2").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "sum v1 mean v3 by id3" # q3
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id3, sum(v1) as v1, mean(v3) as v3 from x group by id3").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = list(spark.sql("select sum(v1) as v1, sum(v3) as v3 from ans").collect()[0].asDict().values())
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id3, sum(v1) as v1, mean(v3) as v3 from x group by id3").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = list(spark.sql("select sum(v1) as v1, sum(v3) as v3 from ans").collect()[0].asDict().values())
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "mean v1:v3 by id4" # q4
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id4, mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from x group by id4").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = list(spark.sql("select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from ans").collect()[0].asDict().values())
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id4, mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from x group by id4").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = list(spark.sql("select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from ans").collect()[0].asDict().values())
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "sum v1:v3 by id6" # q5
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = list(spark.sql("select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from ans").collect()[0].asDict().values())
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = list(spark.sql("select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from ans").collect()[0].asDict().values())
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

#question = "median v3 sd v3 by id4 id5" # q6 # median not yet implemented https://issues.apache.org/jira/browse/SPARK-26589
#gc.collect()
#t_start = timeit.default_timer()
#ans = spark.sql("select id4, id5, median(v3) as median_v3, stddev(v3) as sd_v3 from x group by id4, id5").persist(pyspark.StorageLevel.MEMORY_ONLY)
#print((ans.count(), len(ans.columns)), flush=True) # shape
#t = timeit.default_timer() - t_start
#m = memory_usage()
#ans.createOrReplaceTempView("ans")
#t_start = timeit.default_timer()
#chk = list(spark.sql("select sum(median_v3) as median_v3, sum(sd_v3) as sd_v3 from ans").collect()[0].asDict().values())
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#ans.unpersist()
#spark.catalog.uncacheTable("ans")
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = spark.sql("select id4, id5, median(v3) as median_v3, stddev(v3) as sd_v3 from x group by id4, id5").persist(pyspark.StorageLevel.MEMORY_ONLY)
#print((ans.count(), len(ans.columns)), flush=True) # shape
#t = timeit.default_timer() - t_start
#m = memory_usage()
#ans.createOrReplaceTempView("ans")
#t_start = timeit.default_timer()
#chk = list(spark.sql("select sum(median_v3) as median_v3, sum(sd_v3) as sd_v3 from ans").collect()[0].asDict().values())
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(ans.head(3), flush=True)
#print(ans.tail(3), flush=True)
#ans.unpersist()
#spark.catalog.uncacheTable("ans")
#del ans

question = "max v1 - min v2 by id3" # q7
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id3, max(v1)-min(v2) as range_v1_v2 from x group by id3").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(range_v1_v2) as range_v1_v2 from ans").collect()[0].asDict()['range_v1_v2']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id3, max(v1)-min(v2) as range_v1_v2 from x group by id3").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(range_v1_v2) as range_v1_v2 from ans").collect()[0].asDict()['range_v1_v2']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "largest two v3 by id6" # q8
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from x where v3 is not null) sub_query where order_v3 <= 2").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(largest2_v3) as largest2_v3 from ans").collect()[0].asDict()['largest2_v3']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from x where v3 is not null) sub_query where order_v3 <= 2").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(largest2_v3) as largest2_v3 from ans").collect()[0].asDict()['largest2_v3']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "regression v1 v2 by id2 id4" # q9
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id2, id4, pow(corr(v1, v2), 2) as r2 from x group by id2, id4").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(r2) as r2 from ans").collect()[0].asDict()['r2']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id2, id4, pow(corr(v1, v2), 2) as r2 from x group by id2, id4").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(r2) as r2 from ans").collect()[0].asDict()['r2']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "sum v3 count by id1:id6" # q10
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = list(spark.sql("select sum(v3) as v3, sum(count) as count from ans").collect()[0].asDict().values())
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = list(spark.sql("select sum(v3) as v3, sum(count) as count from ans").collect()[0].asDict().values())
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

spark.stop()

print("grouping finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)

exit(0)
