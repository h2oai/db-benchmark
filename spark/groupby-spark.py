#!/usr/bin/env python

print("# groupby-spark.py")

import os
import gc
import timeit
import pyspark
from pyspark.sql import SparkSession

exec(open("./helpers.py").read())

ver = pyspark.__version__
git = "" # won't fix: https://issues.apache.org/jira/browse/SPARK-16864
task = "groupby"
solution = "spark"
fun = ".sql"
cache = "TRUE"

data_name = os.environ['SRC_GRP_LOCAL']
src_grp = os.path.join("data", data_name+".csv")
print("loading dataset %s" % data_name)

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
#print(spark.sparkContext._conf.getAll())

x = spark.read.csv(src_grp, header=True, inferSchema='true').persist(pyspark.StorageLevel.MEMORY_ONLY)

print(x.count())

x.createOrReplaceTempView("x")

print("grouping...")

question = "sum v1 by id1" # q1
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
print(ans.head(3))
# print(ans.tail(3)) # as of 2.4.0 still not implemented https://issues.apache.org/jira/browse/SPARK-26433
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "sum v1 by id1:id2" # q2
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
print(ans.head(3))
# print(ans.tail(3)) # as of 2.4.0 still not implemented https://issues.apache.org/jira/browse/SPARK-26433
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "sum v1 mean v3 by id3" # q3
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
print(ans.head(3))
# print(ans.tail(3)) # as of 2.4.0 still not implemented https://issues.apache.org/jira/browse/SPARK-26433
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "mean v1:v3 by id4" # q4
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
print(ans.head(3))
# print(ans.tail(3)) # as of 2.4.0 still not implemented https://issues.apache.org/jira/browse/SPARK-26433
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

question = "sum v1:v3 by id6" # q5
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
print(ans.head(3))
# print(ans.tail(3)) # as of 2.4.0 still not implemented https://issues.apache.org/jira/browse/SPARK-26433
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans

# question = "sum v3 count by id1:id6" # q6
# gc.collect()
# t_start = timeit.default_timer()
# ans = spark.sql("select sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6").persist(pyspark.StorageLevel.MEMORY_ONLY)
# print((ans.count(), len(ans.columns))) # shape
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# ans.createOrReplaceTempView("ans")
# tempchk = spark.sql("select sum(v3) as v3, sum(count) as count from ans").collect()[0].asDict()
# chk = [tempchk['v3'], tempchk['count']]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
# ans.unpersist()
# spark.catalog.uncacheTable("ans")
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# ans = spark.sql("select sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6").persist(pyspark.StorageLevel.MEMORY_ONLY)
# print((ans.count(), len(ans.columns))) # shape
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# ans.createOrReplaceTempView("ans")
# tempchk = spark.sql("select sum(v3) as v3, sum(count) as count from ans").collect()[0].asDict()
# chk = [tempchk['v3'], tempchk['count']]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
# print(ans.head(3))
# # print(ans.tail(3)) # as of 2.4.0 still not implemented https://issues.apache.org/jira/browse/SPARK-26433
# ans.unpersist()
# spark.catalog.uncacheTable("ans")
# del ans

spark.stop()

exit(0)
