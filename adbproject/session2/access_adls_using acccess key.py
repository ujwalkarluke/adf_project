# Databricks notebook source
access_key = dbutils.secrets.get("secretbwtsession","kv-formulaoneaccesskey")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.storageformulaoneproject.dfs.core.windows.net", access_key)

# COMMAND ----------

#spark.read.load("abfss://test@storageformulaoneproject.dfs.core.windows.net/input_data")

display(dbutils.fs.ls("abfss://test@storageformulaoneproject.dfs.core.windows.net"))

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("secretbwtsession")

# COMMAND ----------

dbutils.secrets.get("secretbwtsession","kv-formulaoneaccesskey")

# COMMAND ----------

df = spark.read.csv("abfss://test@storageformulaoneproject.dfs.core.windows.net/input_data/airport3.csv")
df.display()
