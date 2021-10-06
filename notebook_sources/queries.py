# Databricks notebook source

access_key = dbutils.secrets.get(scope = "aws", key = "aws-access-key")
secret_key = dbutils.secrets.get(scope = "aws", key = "aws-secret-key")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

# If you are using Auto Loader file notification mode to load files, provide the AWS Region ID.
aws_region = "us-east-1"
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3." + aws_region + ".amazonaws.com")

# COMMAND ----------

genotype_data = spark.read.parquet("s3a://benceolah-databrick-bucket/tables/genotype1")
genotype_data.registerTempTable("genotype")

# reread same data one more time to avoid caching
genotype_data2= spark.read.parquet("s3a://benceolah-databrick-bucket/tables/genotype2")
genotype_data2.registerTempTable("genotype2")

skewed_genotypes = spark.read.parquet("s3a://benceolah-databrick-bucket/tables/genotype_skewed_data")
skewed_genotypes.registerTempTable("skewed_genotypes")

variant_location_data= spark.read.parquet("s3a://benceolah-databrick-bucket/tables/variant_location")
variant_location_data.registerTempTable("variant_location")

sequence_data_data= spark.read.parquet("s3a://benceolah-databrick-bucket/tables/sequence")
sequence_data_data.registerTempTable("sequence")

assembly_data_spec= spark.read.parquet("s3a://benceolah-databrick-bucket/tables/assembly")
sequence_data_data.registerTempTable("assembly")



# COMMAND ----------

# MAGIC %md
# MAGIC # Discover our Environment (Databricks)
# MAGIC  - Workspace introduction - (Workspace - folder creation - notebook cloning)
# MAGIC  - Notebooks and how to use it
# MAGIC  - Cluster creation
# MAGIC  - Spark UI
# MAGIC  - Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC # Your Todo
# MAGIC Create your own environment based on our instructions. 
# MAGIC 
# MAGIC - Please use your first name as prefix and then underscore(_) crunch (david_crunch).

# COMMAND ----------

# MAGIC %md
# MAGIC # Explain explain
# MAGIC In this step lets talk about how we can peek under the hood and see what Spark does internally using **explain** command.
# MAGIC  - to print the physical plan use df.explain() == df.explain("simple")
# MAGIC  - to print everything use df.explain(True) == df.explain("extended") (Parsed Logical + Analyzed Logical + Optimized Logical + Physical) (try to find Waldo)
# MAGIC  - to print the generated code by Spark df.explain("codegen")
# MAGIC  - to print some statistics of the dataframe you can use df.explain("cost")
# MAGIC  - and last the best **df.explain("formatted")** makes the extended explain humanly readadable

# COMMAND ----------

# MAGIC %md
# MAGIC # Discover our data

# COMMAND ----------

display(genotype_data)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Spark Batch APIs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark abstractions
# MAGIC #### 1.) Spark Core / RDD  
# MAGIC - **R**esilient **D**istributed **D**ataset is a distributed collection of objects on memory and disk
# MAGIC - fundamental data structure of Spark
# MAGIC - typed
# MAGIC - offers low-level functionality and control
# MAGIC - only use when you want a total control over your dataset
# MAGIC 
# MAGIC 
# MAGIC #### 2.) SparkSQL / DataFrames
# MAGIC - immutable distributed collection of data just like RDD
# MAGIC - higher abstraction level than Spark Core
# MAGIC - representation: unlike an RDD, data is organized into named columns, it looks like the RDBMS tables we used to
# MAGIC - Schema inference
# MAGIC - SQL functionality
# MAGIC - Query plan optimization (Catalyst and Tungsten optimizers)
# MAGIC - ML library
# MAGIC 
# MAGIC <img style="float: right;" src="https://databricks.com/wp-content/uploads/2015/02/Screen-Shot-2015-02-16-at-9.46.39-AM.png" width="30%" height="30%">
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC #### 3.)  DataSets
# MAGIC - Best from both world - it combines RDD and DF approaches
# MAGIC - still abstract but type-safe
# MAGIC - Not available in Python (some functionality merged with DF)

# COMMAND ----------

data_root_dir = "s3a://benceolah-databrick-bucket/tables/dm/"

# COMMAND ----------

# DBTITLE 1,First load Genotype data into Spark DataFrame
# load the genotype dataset into memory, we use coalesce to distribute data evenly between nodes to avoid data possible data skew
genotype_data = spark.read.parquet(data_root_dir + "genotype").coalesce(4*spark.sparkContext.defaultParallelism).cache()

# COMMAND ----------

# DBTITLE 1,Take a look at the data
display(genotype_data)

# COMMAND ----------

# DBTITLE 1,Convert Genotype to RDD
genotype_data_rdd = genotype_data.rdd.cache()

# COMMAND ----------

# DBTITLE 1,Cache the data in memory (Check Storage)
(genotype_data.count(), genotype_data_rdd.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDD vs DataFrame

# COMMAND ----------

# DBTITLE 1,Let's take a look at genotype RDD
genotype_data_rdd.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### RDD Exercise1
# MAGIC Now try to do a simple exercise on genotype rdds. Calculate the *sum* of total_read_depth for every **id** using RDD.

# COMMAND ----------

def sum_fn(a, b):
    return a + b

# COMMAND ----------

sum_genotype_rdd = genotype_data_rdd.map(lambda row: (row['id'], row['total_read_depth'])).reduceByKey(sum_fn)

# COMMAND ----------

sum_genotype_rdd.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can see it's not that easy to work with RDDs. We need to do extra work on RDD to get the required format for the task at hand.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### RDD Exercise2
# MAGIC Let's take a not that simple task. Calculate the **average** of total_read_depth for every id using RDD.

# COMMAND ----------

avg_genotype_rdd = genotype_data_rdd.map(lambda row: (row['id'], (int(row['total_read_depth']), 1))).reduceByKey(lambda value1, value2: (value1[0]+value2[0], value1[1]+value2[1])).mapValues(lambda x: x[0]/x[1])

# COMMAND ----------

avg_genotype_rdd.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your Todo DF Exercise 1
# MAGIC Lets do the same and try to calculate the **sum** of total_read_depth for every **project_id** in the **genotype_data** DataFrame.

# COMMAND ----------

# DBTITLE 1,Solution for Exercise 1
import pyspark.sql.functions as F

sum_genotype_df = genotype_data.groupBy(F.col("project_id")).agg(F.sum(F.col("total_read_depth")).alias("sum_total_read_depth"))

# COMMAND ----------

display(sum_genotype_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your Todo DF Exercise 2
# MAGIC Lets do the same and try to calculate the **average** of total_read_depth for every **project_id** in the **genotype_data** DataFrame.

# COMMAND ----------

# DBTITLE 1,Solution for Exercise 2
import pyspark.sql.functions as F


avg_genotype_df = genotype_data.groupBy(F.col("project_id")).agg(F.avg(F.col("total_read_depth")).alias("avg_total_read_depth"))

# COMMAND ----------

display(avg_genotype_df)

# COMMAND ----------

# MAGIC %timeit sum_genotype_rdd.collect()

# COMMAND ----------

# MAGIC %timeit avg_genotype_df.agg(F.count("project_id")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC Working with DataFrame instead of RDDs is much more readeable and easier to use (we don't need to extra work to get the required data format) and also the performance gain is huge.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Python or Scala?
# MAGIC 
# MAGIC ## First - don't use RDD
# MAGIC This is the component which will be most affected by the performance of the Python code and the details of PySpark implementation. While Python performance is rather unlikely to be a problem, there at least few factors you have to consider:
# MAGIC 
# MAGIC * Overhead of JVM communication. Practically all data that comes to and from Python executor has to be passed through a socket and a JVM worker. While this is a relatively efficient local communication it is still not free.
# MAGIC * Process-based executors (Python) versus thread based (single JVM multiple threads) executors (Scala). Each Python executor runs in its own process. As a side effect, it provides stronger isolation than its JVM counterpart and some control over executor lifecycle but potentially significantly higher memory usage:
# MAGIC * * interpreter memory footprint
# MAGIC * * footprint of the loaded libraries
# MAGIC less efficient broadcasting (each process requires its own copy of a broadcast)
# MAGIC Performance of Python code itself. Generally speaking Scala is faster than Python but it will vary on task to task.
# MAGIC 
# MAGIC ## Dataframe Python or Scala?
# MAGIC Dataframe are probably the best choice for standard data processing tasks. Since * Python code is mostly limited to high-level logical operations on the driver, there should be no performance difference between Python and Scala*.
# MAGIC 
# MAGIC A single exception is usage of row-wise Python UDFs which are significantly less efficient than their Scala equivalents. While there is some chance for improvements (there has been substantial development in Spark 2.0.0), the biggest limitation is full roundtrip between internal representation (JVM) and Python interpreter. If possible, you should favor a composition of built-in expressions (example. Python UDF behavior has been improved in Spark 2.0.0, but it is still suboptimal compared to native execution.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Predicate pushdown

# COMMAND ----------

# MAGIC %md
# MAGIC ## Goal
# MAGIC Predicate Pushdown gets its name from the fact that portions of SQL statements, ones that filter data, are referred to as predicates.
# MAGIC It can improve query performance by reducing the amount of data read (I/O) from Storage files. The Database process evaluates filter predicates in the query against metadata stored in the Storage files.
# MAGIC 
# MAGIC ** if you can “push down” parts of the query to where the data is stored, and thus filter out most of the data, then you can greatly reduce network traffic **

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Query without pushdown

# COMMAND ----------

spark.conf.set("spark.sql.parquet.filterPushdown",False)


predicate_pushdown = spark.sql("select sum(total_read_depth) from genotype g where g.project_id = 100")
predicate_pushdown.explain("formatted")
predicate_pushdown.collect()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Query with pushdown

# COMMAND ----------

spark.conf.set("spark.sql.parquet.filterPushdown",True)


without_predicate_pushdown = spark.sql("select sum(total_read_depth) from genotype2 g where g.project_id = 100")
without_predicate_pushdown.explain()
without_predicate_pushdown.collect()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Your TODOs
# MAGIC 
# MAGIC  Can you spot the difference in the explain plan?
# MAGIC 
# MAGIC Please Fill out the following table
# MAGIC 
# MAGIC | Metric | predicate_pushdown | without_predicate_pushdown |
# MAGIC | ----------- | ----------- |  ----------- |
# MAGIC | # of files read | ??? | ??? |
# MAGIC | How many MBs were read | ??? | ??? |
# MAGIC | Difference in execution plan| ??? | ??? |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Best Pratices
# MAGIC 
# MAGIC 1) Beware of **Casts**
# MAGIC 
# MAGIC `df_with_string = predicate_pushdown.withColumn("string_column", predicate_pushdown["total_read_depth"].cast(StringType())).registerAsTempTable("table_with_sting")`
# MAGIC `spark.sql("select sum(total_read_depth) from table_with_sting g where g.project_id < 100")`
# MAGIC 
# MAGIC If total_read_depth is a number pushdown works, if it is a string it won't work
# MAGIC   
# MAGIC 2) Related concept is selection pusdown(selecting columns). 
# MAGIC 
# MAGIC `genotype_data2.select("genotype1")`
# MAGIC 
# MAGIC It works best with columnar file formats (parquet,orc)
# MAGIC 
# MAGIC 3) Read your data in advance and apply filters, use registerAsTempTable to reference already filtered data
# MAGIC 
# MAGIC 4) Check your execution plan

# COMMAND ----------

# MAGIC %md
# MAGIC # Bucketing vs Partitioning

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Exercise:
# MAGIC Use genotype dataset to calculate the average of *total_read_depth* for those projects where the **project_id** is **greater than 10 (10<)** and **allele1** is **A**.

# COMMAND ----------

import pyspark.sql.functions as F

filtered_allele_avg = genotype_data.filter((F.col("total_read_depth") > 10) & (F.col("allele1") == "A")).groupBy("project_id", "allele1").agg(F.avg("total_read_depth").alias("avg")).sort(F.desc(F.col("avg")))

# COMMAND ----------

filtered_allele_avg.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partitioning
# MAGIC  - optimizes reads by storing files in a hierarchy of directories which is based on the partioning key
# MAGIC  - partition pruning: limits the number of files Spark reads based on query fileters, this way spark only reads the subset of the files matching the filters and with it reducing disk I/O
# MAGIC  - **tips**: 
# MAGIC    - The partition columns should be used frequently for filtering and should have a small range of values. 
# MAGIC    - try to avoid too many small files, which make scans less efficient
# MAGIC    - try to avoid too few large files which can hurt parallelism

# COMMAND ----------

# DBTITLE 1,Write the genotype dataset partitioned by project_id
# to partition the data use .partitionBy(col)
# genotype_data.write.partitionBy("project_id").format("parquet").save(data_root_dir + "genotype_partitioned")

# COMMAND ----------

# DBTITLE 1,Let's take a peek into the directory
# MAGIC %md
# MAGIC <img style="float: right;" src="/files/partitioned.png" width="20%" height="20%">

# COMMAND ----------

genotype_partitioned = spark.read.parquet(data_root_dir + "genotype_partitioned")

# COMMAND ----------

import pyspark.sql.functions as F

genotype_10_part = genotype_partitioned.filter((F.col("project_id") == 10))
genotype_10_part.explain()
display(genotype_10_part)

# COMMAND ----------

import pyspark.sql.functions as F

genotype_10 = genotype_data.filter((F.col("project_id") == 10))
genotype_10.explain()
display(genotype_10)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Your TODOs
# MAGIC 
# MAGIC  Can you spot the difference in the explain plans?
# MAGIC 
# MAGIC Please Fill out the following table
# MAGIC 
# MAGIC | Metric | predicate_pushdown | without_predicate_pushdown |
# MAGIC | ----------- | ----------- |  ----------- |
# MAGIC | # of files read | ??? | ??? |
# MAGIC | How many MBs were read | ??? | ??? |
# MAGIC | Difference in execution plan| ??? | ??? |

# COMMAND ----------

filtered_allele_avg_part = genotype_partitioned.filter((F.col("total_read_depth") > 10) & (F.col("allele1") == "A")).groupBy("project_id", "allele1").agg(F.avg("total_read_depth").alias("avg"))

# COMMAND ----------

filtered_allele_avg_part.explain("simple")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bucketing
# MAGIC  - data organization technique like partitioning
# MAGIC  - distributes and groups data across fixed number of buckets
# MAGIC  - can improve joins by avoiding shuffles
# MAGIC  - tables can be bucketed more than one value
# MAGIC  - the directory sturcture stays the same
# MAGIC  - optimizes queries, joints and aggregations on the bucketed value
# MAGIC  - good fit for skewed data
# MAGIC  - **tips**: columns which are often used in queries and has high selectivity are good choices for bucketing

# COMMAND ----------

# spark.sql("ANALYZE TABLE genotype_bucketed COMPUTE STATISTICS")
genotype_partitioned_bucketed = spark.table("genotype_bucketed")

# COMMAND ----------

# MAGIC %md
# MAGIC Task is to calculate how many records for **project_id** 10 with less than 10 **total_read_depth** and value A at **allele1** attribute.

# COMMAND ----------

count_orig = genotype_data.filter((F.col("total_read_depth") < 10) & (F.col("allele1") == "A") & (F.col("project_id") == 10)).groupBy("total_read_depth", "project_id").agg(F.count("id").alias("count"))

# COMMAND ----------

count_orig.explain("formatted")

# COMMAND ----------

# to bucket the data use .bucketBy(int, col(s))
# genotype_data.write.sortBy("id").partitionBy("total_read_depth").bucketBy(500, "project_id").format("parquet").option("path", data_root_dir + "genotype_partitioned_bucketed").saveAsTable("genotype_bucketed")

# COMMAND ----------

# MAGIC %md
# MAGIC <img style="float: right;" src="/files/bucketed.png" width="20%" height="20%">

# COMMAND ----------

count_bucketed = genotype_partitioned_bucketed.filter((F.col("total_read_depth") < 10) & (F.col("allele1") == "A") & (F.col("project_id") == 10)).groupBy("total_read_depth", "project_id").agg(F.count("id").alias("count"))

# COMMAND ----------

count_bucketed.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC # 10-15 mins BREAK

# COMMAND ----------

# MAGIC %md
# MAGIC # Broadcast

# COMMAND ----------

# MAGIC %md
# MAGIC - can be used when you are joining a large dataset with a *small* one 
# MAGIC - one of the join relations are being copied and sent to all the worker nodes
# MAGIC - spark use this strategy by default when one of the dataset size is less than 10 mb (you can define higher threshold value `spark.sql.autoBroadcastJoinThreshold`)
# MAGIC - the broadcasted table shared among the executors using P2P protocol
# MAGIC - you can also [use a hint](https://spark.apache.org/docs/3.0.0/sql-ref-syntax-qry-select-hints.html#join-hints) to enforce broadcast join 
# MAGIC 
# MAGIC drawbacks:
# MAGIC - network heavy operation could cause OOM error when used incorrectly (for example when the broadcasted table is to large)

# COMMAND ----------

import pyspark.sql.functions as F

variant = spark.read.parquet(data_root_dir + "variant_location")
sequence = spark.read.parquet(data_root_dir + "sequence")

variant_with_sequence = variant.join(sequence.withColumnRenamed("id", "sequence_id"), ["sequence_id"], "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Your Todo 
# MAGIC Question: how would you check that Spark used BROADCAST JOINS?

# COMMAND ----------

variant_with_sequence.explain()

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  #turn off auto broadcasting

# COMMAND ----------

# MAGIC %timeit variant.join(sequence.withColumnRenamed("id", "sequence_id"), ["sequence_id"], "inner").agg(F.count("sequence_id")).show()

# COMMAND ----------

variant_with_sequence_broadcasted = variant.join(F.broadcast(sequence.withColumnRenamed("id", "sequence_id")), ["sequence_id"], "inner")
variant_with_sequence_broadcasted.explain()

# COMMAND ----------

# MAGIC %timeit variant.join(F.broadcast(sequence.withColumnRenamed("id", "sequence_id")), ["sequence_id"], "inner").agg(F.count("sequence_id")).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Skewed data - key salting

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Goal
# MAGIC 
# MAGIC Skewness is the statistical term, which refers to the value distribution in a given dataset. When we say that the data is highly skewed, it means that some column values have more rows and some very few, i.e the data is not properly/evenly distributed. This affects the performance and parallelism in any distributed system. 
# MAGIC 
# MAGIC Illustration:
# MAGIC 
# MAGIC ![Only for illustration, job stuck at the last stage](https://miro.medium.com/max/1400/1*yK2Ek0WPoezj9WDfAMnGHQ.png)
# MAGIC 
# MAGIC ![Only for illustration, job stuck at the last stage](https://miro.medium.com/max/1400/1*SICi8EJBHIpWzeQvBb1Jog.png)
# MAGIC 
# MAGIC 
# MAGIC So why is this an issue? Your cluster can remain unutilized. This can be a problem during joins and also during aggregation/grouping. 
# MAGIC There are multiple techniques how to solve it, one of them is salting.
# MAGIC 
# MAGIC ### What is salting?
# MAGIC The idea is to invent a new key, which guarantees an even distribution of data. Bellow, you can see the possible ‘key salting’ implementation. 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Original query

# COMMAND ----------

spark.sql("select * from skewed_genotypes g join variant_location vl on vl.id = g.variant_location_id").count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Rewrite the query using salting

# COMMAND ----------

spark.sql("select concat(variant_location_id,'_',FLOOR(RAND(123456)*5)) as variant_location_id_salted from skewed_genotypes").registerTempTable("skewed_genotypes_salted")

variant_location_salted = spark.sql("select id,explode(array(0,1,2,3,4,5)) as salt, concat(id,'_',salt) as variant_location_id_salted  from variant_location")
variant_location_salted.registerTempTable("variant_location_salted")



spark.sql("select * from skewed_genotypes_salted g join variant_location_salted vl on vl.variant_location_id_salted = g.variant_location_id_salted").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Todo
# MAGIC Questions:
# MAGIC 1. Why is this lasting longer than the original query? 
# MAGIC 2. How can we make it faster?

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Influencing length of the tasks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Goal
# MAGIC 
# MAGIC ### SPARK Tasks
# MAGIC The tasks are created by stages; which is the smallest unit in the execution in the Spark applications. The each task represent the local computation, where all of the tasks in a stage have same code but will be executed parallel in different fragments of the dataset.
# MAGIC 
# MAGIC ![alt text](https://media-exp1.licdn.com/dms/image/C4E12AQG6gZaXT4AeAw/article-inline_image-shrink_1000_1488/0/1520213594085?e=1638403200&v=beta&t=H_C0-D_5U32MtgAqC3OwtCRTwRp45SflPbeYTfWgfOw "TASKS")
# MAGIC 
# MAGIC 
# MAGIC The process of distributing these tasks is achieved  by the TaskScheduler and differs based on the fair scheduler or FIFO scheduler. The FIFO schedule is the default Spark scheduler. And in simple way one stage can be computed without moving data across the partitions, but within one stage, the tasks are the units of work done for each partition of the data.
# MAGIC 
# MAGIC ### How should we optimize?
# MAGIC Duration and number of tasks should be optmized. Imagine you have tasks taking 1-2 milliseconds, but in an other stage they are taking 10s of minutes - this is a problem, causing:
# MAGIC 1. Executors to starve - let's say you have 8 executors with 8 slots and you have 10 tasks - you have to wait 8 tasks to finish before you can start the other 2
# MAGIC 2. Inefficient resource allocation - in case of lengthy tasks it is not possible to fully utilize the cluster
# MAGIC 3. Overhead of task scheduling - if tasks are to short the % spent on effective work is low, due to scheduling + resource allocation
# MAGIC 
# MAGIC ** As a rule of thumb (not a hard rule), try to achieve 1-2 minutes tasks **

# COMMAND ----------

# MAGIC %md
# MAGIC ## Original query

# COMMAND ----------



result = spark.sql(("select count(*),a.id as assembly_id, g.allele1 as allele from genotype g "
          "inner join variant_location vl on vl.id=g.variant_location_id "
          "inner join sequence s on s.id = vl.sequence_id "
          "inner join assembly a on a.id = s.assembly_id "
          "group by a.id, g.allele1"))

result.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Your Todo Query to optimize 
# MAGIC 
# MAGIC See [Spark Documentation] (https://spark.apache.org/docs/latest/configuration.html) on how to reduce shuffle partitions.

# COMMAND ----------

spark.conf.set("???",16)

result = spark.sql(("select count(*),a.id as assembly_id, g.allele1 as allele from genotype g "
          "inner join variant_location vl on vl.id=g.variant_location_id "
          "inner join sequence s on s.id = vl.sequence_id "
          "inner join assembly a on a.id = s.assembly_id "
          "group by a.id, g.allele1"))

result.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Note: this change is global (which is usually not the best for all scenarios). Fortunately, we can be more specific [using hints](https://dwgeek.com/spark-sql-and-dataframe-hints-types-usage-and-examples.html/).  
# MAGIC 
# MAGIC Please check which hint would help in this scenario (you do not have to try it out now).

# COMMAND ----------

# MAGIC %md
# MAGIC # UDFs vs PandasUDFs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark UDF
# MAGIC  - invokes the Python function on row by row basis
# MAGIC  - high serialization and invocation overhead
# MAGIC  - data is serialized using Pickle

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Pandas UDF
# MAGIC  - introduced in Spark 2.3
# MAGIC  - Provides Scalar and Grouped map operations
# MAGIC  - invokes the Pandas function block by block (can increase performance up to 3x-100x compared to Python UDFs)
# MAGIC  - Arrow serialization --> In memory columnar format for data analysis
# MAGIC  - Enables Pandas code distribution
# MAGIC  
# MAGIC  note: in case of pd.series to pd.series use case the input and output series must have the same size.
# MAGIC 
# MAGIC <img style="float: right;" src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" width="30%" height="30%">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise1 UDF
# MAGIC Create a UDF which multiply the value of *total_read_depth* by 2 (*2) and create the column double

# COMMAND ----------

genotype_data = spark.read.parquet(data_root_dir + "genotype").coalesce(4*spark.sparkContext.defaultParallelism).cache()

# COMMAND ----------

from pyspark.sql.types import IntegerType

@F.udf(IntegerType())
def double_udf(x):
    return x * 2

# COMMAND ----------

genotype_double_udf = genotype_data.withColumn("double", double_udf("total_read_depth"))

# COMMAND ----------

display(genotype_double_udf)

# COMMAND ----------

genotype_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise2 Pandas
# MAGIC Now try to speed things up and create Pandas UDF which multiply the value of *total_read_depth* by 2 (*2) and create the column double

# COMMAND ----------

from typing import Iterator
import pandas as pd

@F.pandas_udf("long")
def double_pd(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for x in batch_iter:
        yield x * 2

# COMMAND ----------

genotype_double_pd = genotype_data.withColumn("double", double_pd("total_read_depth"))

# COMMAND ----------

display(genotype_double_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benchmark 1
# MAGIC Now lets see which performs better

# COMMAND ----------

# MAGIC %timeit genotype_data.withColumn("double", double_udf("total_read_depth")).agg(F.count("double")).show()

# COMMAND ----------

# MAGIC %timeit genotype_data.withColumn("double", double_pd("total_read_depth")).agg(F.count("double")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise3 UDF
# MAGIC Now lets try to create a UDF which calculates the **mean** of *total_read_depth* grouped by *project_id*

# COMMAND ----------

from pyspark.sql.types import DoubleType
import statistics as stat

@F.udf(DoubleType())
def mean_udf(x):
    return stat.mean(x)

# COMMAND ----------

genotype_mean_udf = genotype_data.groupby(F.col("project_id")).agg(mean_udf((F.collect_list("total_read_depth"))).alias("mean"))

# COMMAND ----------

display(genotype_mean_udf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4
# MAGIC Now lets try to create a Pandas UDF which calculates the **mean** of *total_read_depth* grouped by *project_id*

# COMMAND ----------

import pandas as pd

@F.pandas_udf("double")
def mean_pd(v: pd.Series) -> float:
    return v.mean()

# COMMAND ----------

genotype_mean_pd = genotype_data.groupby(F.col("project_id")).agg(mean_pd("total_read_depth").alias("mean"))

# COMMAND ----------

display(genotype_mean_pd)

# COMMAND ----------

genotype_mean_spark = genotype_data.groupby(F.col("project_id")).agg(F.mean("total_read_depth").alias("mean"))

# COMMAND ----------

display(genotype_mean_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benchmark 2
# MAGIC Now lets see which performs better

# COMMAND ----------

# MAGIC %timeit genotype_data.agg(mean_udf((F.collect_list("total_read_depth"))).alias("mean")).agg(F.count("mean")).show()

# COMMAND ----------

# MAGIC %timeit genotype_data.agg(mean_pd("total_read_depth").alias("mean")).agg(F.count("mean")).show()

# COMMAND ----------

# DBTITLE 1,Now lets compare it with Spark built in functions
# MAGIC %timeit genotype_data.agg(F.mean("total_read_depth").alias("mean")).agg(F.count("mean")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion:
# MAGIC 
# MAGIC As we have seen in the previous tasks that usage of Pandas UDF in Spark is the same as the good old Spark UDFs but yields significatnly better reasults without major effort it also opens up a new range of functionality of Pandas library. We have also seen that nothing can beat the in-built Spark functions so try to use everytime.
# MAGIC 
# MAGIC **note:** Data partitions in Spark are converted into Arrow record batches, which can temporarily lead to high memory usage in the JVM. To avoid possible out of memory exceptions, you can adjust the size of the Arrow record batches by setting the `spark.sql.execution.arrow.maxRecordsPerBatch` configuration to an integer that determines the maximum number of rows for each batch.
# MAGIC 
# MAGIC ## Conclusion2: 
# MAGIC Avoid using UDFs and use built-in functions like Map, Aggeragete... etc
# MAGIC References:
# MAGIC 1) [see benchmark here ](https://towardsdatascience.com/performance-in-apache-spark-benchmark-9-different-techniques-955d3cc93266)
# MAGIC 2) [see details about code generation](https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Spark in the cloud
# MAGIC 
# MAGIC ## AWS Glue
# MAGIC AWS Glue is a serverless data integration service that makes it easy to discover, prepare, and combine data for analytics, machine learning, and application development. AWS Glue provides all the capabilities needed for data integration so that you can start analyzing your data and putting it to use in minutes instead of months.
# MAGIC 
# MAGIC Pros
# MAGIC + Managed service
# MAGIC + Ci/Cd requires extra attention
# MAGIC 
# MAGIC Cons
# MAGIC + can be $
# MAGIC + config is limited, won't scale as much as EMR/Vanila spark
# MAGIC 
# MAGIC 
# MAGIC # Using object storage - your path matters
# MAGIC  Amazon S3 automatically scales to high request rates. For example, your application can achieve at least 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second per prefix in a bucket.
# MAGIC *Sustained * load is needed to S3 to scale
# MAGIC  
# MAGIC Prefix patterns:
# MAGIC ```
# MAGIC 1) s3://.../iot_device_id/year/month
# MAGIC 2) s3://.../year/month/iot_device_id
# MAGIC 3) s3://.../month/year/iot_device_id
# MAGIC ```
# MAGIC 
# MAGIC Which one is the best?

# COMMAND ----------

# MAGIC %md
# MAGIC # Questions?

# COMMAND ----------

# MAGIC %md
# MAGIC # Your Todo 
# MAGIC Optional (or homework) there is the following underperforming Spark query try to optimize it.

# COMMAND ----------

result = spark.sql(("select s.id,count(g.allele1),g.allele1 from genotype g "
          "inner join variant_location vl on vl.id=g.variant_location_id "
          "inner join sequence s on s.id = vl.sequence_id "
          "inner join assembly a on a.id = s.assembly_id "
          "group by a.id,s.id,g.allele1"))
