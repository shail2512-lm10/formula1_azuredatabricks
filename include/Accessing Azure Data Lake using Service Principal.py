# Databricks notebook source
# MAGIC %md
# MAGIC #Steps to Access Azure Data Lake in Databricks Using Service Principal
# MAGIC 1. Register Azure AD App/Service Prinicapal
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set Spark config with App/client ID, Directory/Tenant ID, Client Secret (add all 3 to key vault and then db secret scope)
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage#--access-azure-data-lake-storage-gen2-or-blob-storage-using-oauth-20-with-an-azure-service-principal
# MAGIC 4. Assign the role "Storage Blob Data Contributor" to the Data lake for the created service principal in 1st step. 

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-secretscope", key="client-id-formula1-app")
tenant_id = dbutils.secrets.get(scope="formula1-secretscope", key="tenant-id-formula1-app")
client_secret = dbutils.secrets.get(scope="formula1-secretscope", key="client-secret-formula1-app")

# COMMAND ----------

#service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.formula1dlshail.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlshail.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlshail.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlshail.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlshail.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@formula1dlshail.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://bronze@formula1dlshail.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

