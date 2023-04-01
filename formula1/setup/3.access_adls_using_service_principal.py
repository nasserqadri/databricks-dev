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

client_id=dbutils.secrets.get(scope='formula1-scope', key='formula1-client-id')
tenant_id=dbutils.secrets.get(scope='formula1-scope', key='formula1-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-client-secret')


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlnq.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlnq.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlnq.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlnq.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlnq.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1dlnq.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@formula1dlnq.dfs.core.windows.net/circuits.csv'))
