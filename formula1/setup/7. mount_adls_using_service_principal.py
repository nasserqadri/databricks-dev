# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Mount Azure Data Lake using service principal
# MAGIC 
# MAGIC 1. Get client_id, tenant_id, and client_secret from key vault
# MAGIC 2. Set spark cfg with app/client ID, directory/tenant ID and secre
# MAGIC 3. CAll file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id=dbutils.secrets.get(scope='formula1-scope', key='formula1-client-id')
tenant_id=dbutils.secrets.get(scope='formula1-scope', key='formula1-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-client-secret')


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}



# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demo@formula1dlnq.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlnq/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1dlnq/demo'))

# COMMAND ----------

display(spark.read.csv('/mnt/formula1dlnq/demo/circuits.csv'))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dlnq/demo')

# COMMAND ----------

display(dbutils.fs.mounts())
