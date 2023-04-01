# Databricks notebook source
# MAGIC %md
# MAGIC ## Explore the DBFS root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 2. Interact with the DBFS File Browser
# MAGIC 3. Upload file to DBFS root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))
