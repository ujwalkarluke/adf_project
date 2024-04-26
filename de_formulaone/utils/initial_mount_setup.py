# Databricks notebook source
# MAGIC %md
# MAGIC #### Mounting Bronze layer to ADB

# COMMAND ----------

dbutils.widgets.text("layer_name", "silver")
layer_name = dbutils.widgets.get("layer_name")

# COMMAND ----------

print(layer_name)

# COMMAND ----------

dbutils.secrets.list('secretbwtsession')

# COMMAND ----------

application_id = dbutils.secrets.get("secretbwtsession","kv-bwtformulaone-application-id")
directory_id = dbutils.secrets.get("secretbwtsession","kv-bwtformulaone-directory-id")
secret_credential_key_name = dbutils.secrets.get("secretbwtsession","kv-bwtformulaone-service-credential")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": application_id,
           "fs.azure.account.oauth2.client.secret": secret_credential_key_name,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

dbutils.fs.mount(
  source = f"abfss://{layer_name}@storageformulaoneproject.dfs.core.windows.net/",
  mount_point = f"/mnt/{layer_name}",
  extra_configs = configs)

# COMMAND ----------

#display(dbutils.fs.ls(f"/mnt/{layer_name}"))

# COMMAND ----------

dbutils.fs.mounts()
