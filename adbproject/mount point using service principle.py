# Databricks notebook source
dbutils.secrets.list("secretbwtsession")

# COMMAND ----------

application_id = dbutils.secrets.get("secretbwtsession","kv-bwtformulaone-application-id")
directory_id = dbutils.secrets.get("secretbwtsession","kv-bwtformulaone-directory-id")
secret_credential = dbutils.secrets.get("secretbwtsession","kv-bwtformulaone-service-credential")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": application_id,
           "fs.azure.account.oauth2.client.secret": secret_credential,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://test@storageformulaoneproject.dfs.core.windows.net/",
  mount_point = "/mnt/test",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/test"))
