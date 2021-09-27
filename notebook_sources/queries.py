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
# MAGIC 
# MAGIC # --------- Part 1

# COMMAND ----------

# MAGIC %md
# MAGIC # Discover our data

# COMMAND ----------

display(genotype_data)

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

spark.conf.set("spark.sql.parquet.filterPushdown",true)


without_predicate_pushdown = spark.sql("select sum(total_read_depth) from genotype2 g where g.project_id = 100")
without_predicate_pushdown.explain()
without_predicate_pushdown.collect()

# COMMAND ----------

# MAGIC %md 

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

display(genotype_data)

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
# MAGIC ## Todo
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



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Query to optimize 
# MAGIC 
# MAGIC See [Spark Documentation] (https://spark.apache.org/docs/latest/configuration.htmt) on how to reduce shuffle partitions.

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


