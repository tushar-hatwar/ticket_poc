# Databricks notebook source
storage_account_name = "saticketpocdata08062023"
client_id            = "0aad77b6-a9ff-45e3-a931-c16111e79468"
tenant_id            = "a9a1ad60-0851-40df-8e61-d9dd36cf0e8b"
client_secret        = "TA_8Q~MKwv1rOtO_SAcWPDIU-NEfzQxF2~Kzsbd~"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

# mounting the lake container 
mount_adls("lake")

# COMMAND ----------

# Checking all the mounts
display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storage_account_name}/lake/"))

# COMMAND ----------

dbutils.fs.unmount(f"/mnt/{storage_account_name}/lake")

# COMMAND ----------


