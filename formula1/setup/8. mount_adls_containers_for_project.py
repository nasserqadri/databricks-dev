# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Mount Azure Data Lake Containers for the project
# MAGIC 
# MAGIC 1. Get client_id, tenant_id, and client_secret from key vault
# MAGIC 2. Set spark cfg with app/client ID, directory/tenant ID and secre
# MAGIC 3. CAll file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    client_id=dbutils.secrets.get(scope='formula1-scope', key='formula1-client-id')
    tenant_id=dbutils.secrets.get(scope='formula1-scope', key='formula1-tenant-id')
    client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-client-secret')
    
    # set spark configs
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    if any(mount.mountPoint== f'/mnt/{storage_account_name}/{container_name}' for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
        
        
    # mount storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs
    )
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Mount raw container first

# COMMAND ----------

mount_adls('formula1dlnq', 'raw')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Mount presentation container first

# COMMAND ----------

mount_adls('formula1dlnq', 'presentation')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Mount processed container first

# COMMAND ----------

mount_adls('formula1dlnq', 'processed')
