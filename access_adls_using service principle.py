# Databricks notebook source
# MAGIC %md 
# MAGIC #### Access ADLS from Databricks using Service Principle

# COMMAND ----------

application_id = "410b5c98-d906-4662-81fd-c9160cf937be"
directory_id = "1a7529a1-0433-4ae9-bb28-00e32ebcd274"
service_credential = "ofY8Q~i5aKCQjs4YsKCuIP9.rd31oUjkGnIF4cg4"

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.storageformulaoneproject.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.storageformulaoneproject.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.storageformulaoneproject.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.storageformulaoneproject.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.storageformulaoneproject.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

#spark.read.load("abfss://test@storageformulaoneproject.dfs.core.windows.net/input_data")

dbutils.fs.ls("abfss://test@storageformulaoneproject.dfs.core.windows.net/input_data/")
