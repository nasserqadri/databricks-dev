# Databricks notebook source
# MAGIC %md
# MAGIC ## Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------



# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope', key='formula1dlnq-account-key')
