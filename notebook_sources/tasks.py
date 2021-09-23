# Databricks notebook source
data_root_dir = "s3://hcom-data-science-prod-ds-image-pipeline/crunch/test/tables/"

# COMMAND ----------

# DBTITLE 1,Load Genotype data into Spark DataFrame
genotype_data = spark.read.parquet(data_root_dir + "genotype3").coalesce(4*spark.sparkContext.defaultParallelism).cache()

# COMMAND ----------

genotype_data.count()

# COMMAND ----------

# DBTITLE 1,Convert Genotype to RDD
genotype_data_rdd = genotype_data.rdd.cache()

# COMMAND ----------

# DBTITLE 1,Cache the data in memory (Check Storage)
(genotype_data.count(), genotype_data_rdd.count())

# COMMAND ----------

from pyspark.sql import types 
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

display(genotype_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDD vs DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calculate the *sum* of total_read_depth for every **id** using RDD.

# COMMAND ----------

def sum_fn(a, b):
    return a + b

# COMMAND ----------

sum_genotype_rdd = genotype_data_rdd.map(lambda row: (row['id'], row['total_read_depth'])).reduceByKey(sum_fn)

# COMMAND ----------

sum_genotype_rdd.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calculate the **average** of total_read_depth for every id using RDD.

# COMMAND ----------

avg_genotype_rdd = genotype_data_rdd.map(lambda row: (row['id'], (int(row['total_read_depth']), 1))).reduceByKey(lambda value1, value2: (value1[0]+value2[0], value1[1]+value2[1])).mapValues(lambda x: x[0]/x[1])

# COMMAND ----------

avg_genotype_rdd.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Task: Lets do the same and Calculate the *sum* of total_read_depth for every **id** using DataFrame.

# COMMAND ----------

sum_genotype_df = genotype_data.groupBy(F.col("id")).agg(F.sum(F.col("total_read_depth")).alias("sum_total_read_depth"))

# COMMAND ----------

display(sum_genotype_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Lets do the same and Calculate the *average* of total_read_depth for every **id** using DataFrame.

# COMMAND ----------

from pyspark.sql import types 
import pyspark.sql.functions as F


avg_genotype_df = genotype_data.groupBy(F.col("id")).agg(F.avg(F.col("total_read_depth")).alias("avg_total_read_depth"))

# COMMAND ----------

display(avg_genotype_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bucketing vs Partitioning

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Task: Use genotype dataset to calculate the average of *total_read_depth* for those projects where the **project_id** is **greater than 10 (10<)** and **allele1** is **A**.

# COMMAND ----------

filtered_allele_avg = genotype_data.filter((F.col("total_read_depth") > 10) & (F.col("allele1") == "A")).groupBy("project_id", "allele1").agg(F.avg("total_read_depth").alias("avg")).sort(F.desc(F.col("avg")))

# COMMAND ----------

filtered_allele_avg.explain()

# COMMAND ----------

display(filtered_allele_avg)

# COMMAND ----------

genotype_data.write.partitionBy("project_id").format("parquet").save(data_root_dir + "genotype_partitioned")

# COMMAND ----------

genotype_data.write.partitionBy("project_id").bucketBy(4,"allele1").format("parquet").option("path", data_root_dir "genotype_partitioned_bucketed").mode("overwrite").saveAsTable("genotype_bucketed")

# COMMAND ----------

# MAGIC %sh
# MAGIC aws s3 ls s3://hcom-data-science-prod-ds-image-pipeline/crunch/test/tables/genotype_partitioned_bucketed/

# COMMAND ----------

genotype_partitioned = spark.read.parquet(data_root_dir + "genotype_partitioned")

# COMMAND ----------

filtered_allele_avg_part = genotype_partitioned.filter((F.col("total_read_depth") > 10) & (F.col("allele1") == "A")).groupBy("project_id", "allele1").agg(F.avg("total_read_depth").alias("avg"))

# COMMAND ----------

filtered_allele_avg_part.explain("simple")

# COMMAND ----------

genotype_partitioned_bucketed = spark.table("genotype_bucketed")

# COMMAND ----------

filtered_allele_avg_part_buck = genotype_partitioned_bucketed.filter((F.col("total_read_depth") > 10) & (F.col("allele1") == "A")).groupBy("project_id", "allele1").agg(F.avg("total_read_depth").alias("avg"))

# COMMAND ----------

filtered_allele_avg_part_buck.explain()

# COMMAND ----------

display(filtered_allele_avg_part)

# COMMAND ----------

# MAGIC %md
# MAGIC # UDFs vs PandasUDFs

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Create a UDF which multiply the value of *total_read_depth* by 2 (*2) and create the column double

# COMMAND ----------

@F.udf(IntegerType())
def double_udf(x):
    return x * 2

# COMMAND ----------

genotype_double_udf = genotype_data.withColumn("double", double_udf("total_read_depth"))

# COMMAND ----------

display(genotype_double_udf)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Now try to speed things up and create Pandas UDF which multiply the value of *total_read_depth* by 2 (*2) and create the column double

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
# MAGIC ###### Now lets see which performs better

# COMMAND ----------

# MAGIC %timeit genotype_data.withColumn("double", double_udf("total_read_depth")).agg(F.count("double")).show()

# COMMAND ----------

# MAGIC %timeit genotype_data.withColumn("double", double_pd("total_read_depth")).agg(F.count("double")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Now lets try to create a UDF which calculates the **mean** of *total_read_depth* grouped by *project_id*

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
# MAGIC ##### Now lets try to create a Pandas UDF which calculates the **mean** of *total_read_depth* grouped by *project_id*

# COMMAND ----------

import pandas as pd

@F.pandas_udf("double")
def mean_pd(v: pd.Series) -> float:
    return v.mean()

# COMMAND ----------

genotype_mean_spark = genotype_data.groupby(F.col("project_id")).agg(F.mean("total_read_depth").alias("mean"))

# COMMAND ----------

genotype_mean_pd = genotype_data.groupby(F.col("project_id")).agg(mean_pd("total_read_depth").alias("mean"))

# COMMAND ----------

display(genotype_mean_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Now lets see which performs better

# COMMAND ----------

# MAGIC %timeit genotype_data.groupby(F.col("project_id")).agg(mean_udf((F.collect_list("total_read_depth"))).alias("mean")).agg(F.count("mean")).show()

# COMMAND ----------

# MAGIC %timeit genotype_data.groupby(F.col("project_id")).agg(mean_pd("total_read_depth").alias("mean")).agg(F.count("mean")).show()

# COMMAND ----------

# DBTITLE 1,Now lets compare it with Spark built in functions
# MAGIC %timeit genotype_data.groupby(F.col("project_id")).agg(F.mean("total_read_depth").alias("mean")).agg(F.count("mean")).show()

# COMMAND ----------



# COMMAND ----------

# https://medium.com/analytics-vidhya/spark-3-understanding-explain-formatted-d4f33c1dee86

# COMMAND ----------

sqlContext.clearCache()

# COMMAND ----------

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000000")

# COMMAND ----------


