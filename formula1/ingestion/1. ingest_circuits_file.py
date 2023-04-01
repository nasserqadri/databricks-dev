# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 1 - read csv file using spark df reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlnq/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), False),
                                     StructField("name", StringType(), False),
                                     StructField("location", StringType(), False),
                                     StructField("country", StringType(), False),
                                     StructField("lat", DoubleType(), False),
                                     StructField("lng", DoubleType(), False),
                                     StructField("alt", IntegerType(), False),
                                     StructField("url", StringType(), False),
                                    ])

# COMMAND ----------

circuits_df=spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv('dbfs:/mnt/formula1dlnq/raw/circuits.csv')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------


