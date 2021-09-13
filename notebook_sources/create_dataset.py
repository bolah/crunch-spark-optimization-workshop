# Databricks notebook source
access_key = dbutils.secrets.get(scope = "aws", key = "aws-access-key")
secret_key = dbutils.secrets.get(scope = "aws", key = "aws-secret-key")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

# If you are using Auto Loader file notification mode to load files, provide the AWS Region ID.
aws_region = "us-east-1"
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3." + aws_region + ".amazonaws.com")

# COMMAND ----------



# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import FloatType, IntegerType, StringType


# COMMAND ----------



# COMMAND ----------

genotype_row_count = 1000 * 1000000
genotype_data_spec = (dg.DataGenerator(spark, name="genotype", rows=genotype_row_count,
                                  partitions=100, randomSeedMethod='hash_fieldname', 
                                  verbose=True)
                   .withIdOutput()
                   .withColumn("variant_location_id", IntegerType())
                   .withColumn("material_alias", StringType(),template="aaaaxxxx", baseColumn="id")
                   .withColumn("project_id",IntegerType(), maxValue=1000)
                   .withColumn("total_read_depth",minValue=0, maxValue=100, random=True)
                   .withColumn("allele1", StringType(), values=['A', 'T', 'C', 'G'], random=True)
                   .withColumn("allele2", StringType(), values=['A', 'T', 'C', 'G'], random=True)
                                                        )

genotype_data = genotype_data_spec.build()
display(genotype_data)

# COMMAND ----------

variant_location_row_count = genotype_row_count/10
sequence_row_count = 100

variant_data_spec = (dg.DataGenerator(spark, name="variant_location", rows=variant_location_row_count,
                                  partitions=4, randomSeedMethod='hash_fieldname', 
                                  verbose=True)
                   .withIdOutput()
                   .withColumn("sequence_id",IntegerType(), maxValue=sequence_row_count)
                   .withColumn("position",minValue=0, maxValue=249*1000000, random=True)
                   .withColumn("variant_type_code", StringType(), values=['I', 'D', 'S', 'G'], random=True)
                   .withColumn("species_id", IntegerType(), maxValue=1000)
                   .withColumn("strand", StringType(), values=['top', 'plus', 'minus'], random=True)

                   )

variant_location_data = variant_data_spec.build()
variant_location_data.head(10)

# COMMAND ----------

display(variant_location_data)

# COMMAND ----------



# COMMAND ----------

assembly_row_count=2
sequence_data_spec = (dg.DataGenerator(spark, name="sequence", rows=sequence_row_count,
                                  partitions=1, randomSeedMethod='hash_fieldname', 
                                  verbose=True)
                   .withIdOutput()
                   .withColumn("sequence_name",StringType(), template="\\chromosome-DD")
                   .withColumn("assembly_id", IntegerType(), maxValue=assembly_row_count)

                   )

sequence_data_data = sequence_data_spec.build()
sequence_data_data.head(10)


# COMMAND ----------



assembly_data_spec = (dg.DataGenerator(spark, name="assembly", rows=assembly_row_count,
                                  partitions=1, randomSeedMethod='hash_fieldname', 
                                  verbose=True)
                   .withIdOutput()
                   .withColumn("assembly_name",StringType(), StringType(), values=['corn_v3', 'corn_v5'])

                   )

assembly_data_spec = sequence_data_spec.build()
assembly_data_spec.head(10)


genotype_data.registerTempTable("genotype")
variant_location_data.registerTempTable("variant_location")
sequence_data_data.registerTempTable("sequence")
assembly_data_spec.registerTempTable("assembly")

# COMMAND ----------

result = spark.sql(("select * from genotype g "
          "inner join variant_location vl on vl.id=g.variant_location_id "
          "inner join sequence s on s.id = vl.sequence_id "
          "inner join assembly a on a.id = s.assembly_id " ))

# COMMAND ----------



genotype_data.write.mode('overwrite').parquet("s3a://benceolah-databrick-bucket/tables/genotype3")
variant_location_data.write.mode('overwrite').parquet("s3a://benceolah-databrick-bucket/tables/variant_location")
sequence_data_data.write.mode('overwrite').parquet("s3a://benceolah-databrick-bucket/tables/sequence")
assembly_data_spec.write.mode('overwrite').parquet("s3a://benceolah-databrick-bucket/tables/assembly")

# COMMAND ----------


