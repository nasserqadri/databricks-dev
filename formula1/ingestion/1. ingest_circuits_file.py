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
from pyspark.sql.functions import col

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

# MAGIC %md 
# MAGIC ### Select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), 
                                          col("circuitRef"), 
                                          col("name"), 
                                          col('location'),
                                          col('country'),
                                          col('lat'),
                                          col('lng'),
                                          col('alt')
                                         )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Rename the columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")

# COMMAND ----------

display(circuits_renamed_df)
