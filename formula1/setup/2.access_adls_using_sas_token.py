# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Access Azure Data Lake using SAS token
# MAGIC 
# MAGIC 1. Set up the spark config for SAS token
# MAGIC 2. list files from the  container
# MAGIC 3. Read data from the circuits.csv file

# COMMAND ----------

saskey = dbutils.secrets.get(scope='formula1-scope', key='formula1-sas')

# COMMAND ----------

spark.conf.set('fs.azure.account.auth.type.formula1dlnq.dfs.core.windows.net', 'SAS')
spark.conf.set('fs.azure.sas.token.provider.type.formula1dlnq.dfs.core.windows.net', 'org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider')
spark.conf.set('fs.azure.sas.fixed.token.formula1dlnq.dfs.core.windows.net', saskey)

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1dlnq.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@formula1dlnq.dfs.core.windows.net/circuits.csv'))
