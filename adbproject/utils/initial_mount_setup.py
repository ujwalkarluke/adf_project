# Databricks notebook source
dbutils.widgets.text("layer_name", "silver")
dbutils.widgets.text("storage_account_name", "formulaone")
layer_name = dbutils.widgets.get("layer_name")
storage_account_name = dbutils.widgets.get("storage_account_name")

# COMMAND ----------

dbutils.secrets.list('secretbwtsession')

# COMMAND ----------

application_id = dbutils.secrets.get("secretbwtsession","kv-bwtformulaone-application-id")
directory_id = dbutils.secrets.get("secretbwtsession","kv-bwtformulaone-directory-id")
secret_credential_key_name = dbutils.secrets.get("secretbwtsession","kv-bwtformulaone-service-credential")

# COMMAND ----------

mount_point = f"/mnt/{storage_account_name}/{layer_name}"

# Check if the mount point is already mounted
if len(list(filter(lambda mount: mount.mountPoint == mount_point, dbutils.fs.mounts()))) > 0:
    # Unmount the directory
    dbutils.fs.unmount(mount_point)

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": secret_credential_key_name,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"
}

# Mount the directory
dbutils.fs.mount(
    source=f"abfss://{storage_account_name}@storageformulaoneproject.dfs.core.windows.net/{layer_name}",
    mount_point=mount_point,
    extra_configs=configs
)

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storage_account_name}/{layer_name}"))

# COMMAND ----------

dbutils.fs.mounts()
