# Databricks notebook source


access_key = dbutils.secrets.get(scope = "aws", key = "aws-access-key")
secret_key = dbutils.secrets.get(scope = "aws", key = "aws-secret-key")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

# If you are using Auto Loader file notification mode to load files, provide the AWS Region ID.
aws_region = "us-east-1"
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3." + aws_region + ".amazonaws.com")

# COMMAND ----------

genotype_data= spark.read.parquet("s3a://benceolah-databrick-bucket/tables/genotype")
variant_location_data= spark.read.parquet("s3a://benceolah-databrick-bucket/tables/variant_location")
sequence_data_data= spark.read.parquet("s3a://benceolah-databrick-bucket/tables/sequence")
assembly_data_spec= spark.read.parquet("s3a://benceolah-databrick-bucket/tables/assembly")


# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Predicate pushdown

# COMMAND ----------

genotype_data.registerTempTable("genotype")
spark.sql("select sum(total_read_depth) from genotype g").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ** Please check how many MB were read from filesystem **

# COMMAND ----------

spark.catalog.clearCache()

query = spark.sql("select sum(total_read_depth) from genotype g where g.project_id = 100")

query.collect()

# COMMAND ----------

query.explain(mode="formatted")

# COMMAND ----------


