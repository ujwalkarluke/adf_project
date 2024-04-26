# Databricks notebook source
#%run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("run_dt","")
run_dt = dbutils.widgets.get("run_dt")

# COMMAND ----------

from datetime import datetime

#initialize required variables

if run_dt:
    current_dt = run_dt
else:
    current_dt = datetime.now().strftime("%Y%m%d")
input_file_path = f"/mnt/formulaone/bronze/constructors/{current_dt}"
output_file_path = f"/mnt/formulaone/silver/constructors/{current_dt}"

# COMMAND ----------

from pyspark.sql.functions import col, explode

# COMMAND ----------

df = spark.read.json(input_file_path)

# COMMAND ----------

df.columns

# COMMAND ----------

df = df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref")

# COMMAND ----------

df.columns

# COMMAND ----------

df.distinct().write.mode("overwrite").parquet(output_file_path)
