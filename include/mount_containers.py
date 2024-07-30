# Databricks notebook source
def mount_container(container, datalake):

    client_id = dbutils.secrets.get(scope="formula1-secretscope", key="client-id-formula1-app")
    tenant_id = dbutils.secrets.get(scope="formula1-secretscope", key="tenant-id-formula1-app")
    client_secret = dbutils.secrets.get(scope="formula1-secretscope", key="client-secret-formula1-app")

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if not any(mount.mountPoint == f"/mnt/{datalake}/{container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source = f"abfss://{container}@{datalake}.dfs.core.windows.net/",
            mount_point = f"/mnt/{datalake}/{container}",
            extra_configs = configs)
        
    else:
        print("Already mounted")


# COMMAND ----------

mount_container("bronze", "formula1dlshail")
mount_container("silver", "formula1dlshail")
mount_container("gold", "formula1dlshail")

# COMMAND ----------

