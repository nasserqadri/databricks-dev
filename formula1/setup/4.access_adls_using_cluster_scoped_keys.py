# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Access Azure Data Lake using SAS token
# MAGIC 
# MAGIC 1. Register azure AD app/service principal
# MAGIC 2. Generate a secret/password for the app
# MAGIC 3. Set spark cfg with app/client ID, directory/tenant ID and secret
# MAGIC 4. Assign role 'storage blob data contributor' to the data lake

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1dlnq.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@formula1dlnq.dfs.core.windows.net/circuits.csv'))
