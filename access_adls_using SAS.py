# Databricks notebook source
# MAGIC %md 
# MAGIC #### Access ADLS from Databricks using SAS (Shared Access Signature)

# COMMAND ----------

sas_token = r"sp=rl&st=2024-04-02T06:14:31Z&se=2024-04-03T14:14:31Z&spr=https&sv=2022-11-02&sr=c&sig=eQMx52Xkbsb288tI7BsOXllnxtAkOgaRl0y4iGz3%2Bgc%3D"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.storageformulaoneproject.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storageformulaoneproject.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.storageformulaoneproject.dfs.core.windows.net", f"{sas_token}")

# COMMAND ----------

#spark.read.load("abfss://test@storageformulaoneproject.dfs.core.windows.net/input_data")

dbutils.fs.ls("abfss://test@storageformulaoneproject.dfs.core.windows.net/input_data/")
