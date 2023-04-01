# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Access Azure Data Lake using access keys
# MAGIC 
# MAGIC 1. Set up the spark config fs.azure.account.key
# MAGIC 2. list files from the  container
# MAGIC 3. Read data from the circuits.csv file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope='formula1-scope', key='formula1dlnq-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlnq.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1dlnq.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@formula1dlnq.dfs.core.windows.net/circuits.csv'))
