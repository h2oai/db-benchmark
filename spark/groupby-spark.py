#!/usr/bin/env python

print("# groupby-spark.py")

import os
import gc
import timeit
import pyspark
from pyspark.sql import SparkSession

exec(open("./helpers.py").read())

src_grp = os.environ['SRC_GRP_LOCAL']

ver = pyspark.__version__
git = ""
task = "groupby"
data_name = os.path.basename(src_grp)
solution = "spark"
fun = ".sql"
cache = "TRUE"

print("loading dataset...")

from pyspark.conf import SparkConf
spark = SparkSession.builder \
     .master("local[*]") \
     .appName("groupby-spark") \
     .config("spark.executor.memory", "100g") \
     .config("spark.driver.memory", "100g") \
     .config("spark.python.worker.memory", "100g") \
     .config("spark.driver.maxResultSize", "100g") \
     .config("spark.network.timeout", "2400") \
     .config("spark.executor.heartbeatInterval", "1200") \
     .config("spark.ui.showConsoleProgress", "false") \
     .getOrCreate()
print(spark.sparkContext._conf.getAll())

if os.path.isfile(data_name):
  x = spark.read.csv(data_name, header=True, inferSchema='true').persist(pyspark.StorageLevel.MEMORY_ONLY)
else:
  x = spark.read.csv(src_grp, header=True, inferSchema='true').persist(pyspark.StorageLevel.MEMORY_ONLY)

print(x.count())

x.createOrReplaceTempView("x")

print("grouping...")

question = "sum v1 by id1" #1
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select sum(v1) as v1 from x group by id1").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns))) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
ans.createOrReplaceTempView("ans")
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select sum(v1) as v1 from x group by id1").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns))) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
ans.createOrReplaceTempView("ans")
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "sum v1 by id1:id2" #2
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select sum(v1) as v1 from x group by id1, id2").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns))) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
ans.createOrReplaceTempView("ans")
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select sum(v1) as v1 from x group by id1, id2").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns))) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
ans.createOrReplaceTempView("ans")
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "sum v1 mean v3 by id3" #3
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select sum(v1) as v1, mean(v3) as v3 from x group by id3").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns))) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
ans.createOrReplaceTempView("ans")
tempchk = spark.sql("select sum(v1) as v1, sum(v3) as v3 from ans").collect()[0].asDict()
chk = [tempchk['v1'], tempchk['v3']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select sum(v1) as v1, mean(v3) as v3 from x group by id3").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns))) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
ans.createOrReplaceTempView("ans")
tempchk = spark.sql("select sum(v1) as v1, sum(v3) as v3 from ans").collect()[0].asDict()
chk = [tempchk['v1'], tempchk['v3']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "mean v1:v3 by id4" #4
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from x group by id4").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns))) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
ans.createOrReplaceTempView("ans")
tempchk = spark.sql("select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from ans").collect()[0].asDict()
chk = [tempchk['v1'], tempchk['v2'], tempchk['v3']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from x group by id4").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns))) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
ans.createOrReplaceTempView("ans")
tempchk = spark.sql("select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from ans").collect()[0].asDict()
chk = [tempchk['v1'], tempchk['v2'], tempchk['v3']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "sum v1:v3 by id6" #5
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns))) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
ans.createOrReplaceTempView("ans")
tempchk = spark.sql("select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from ans").collect()[0].asDict()
chk = [tempchk['v1'], tempchk['v2'], tempchk['v3']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql("select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6").persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns))) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
ans.createOrReplaceTempView("ans")
tempchk = spark.sql("select sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from ans").collect()[0].asDict()
chk = [tempchk['v1'], tempchk['v2'], tempchk['v3']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

spark.stop()

exit(0)
