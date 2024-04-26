# Databricks notebook source
# MAGIC %run ../utils/commonfunctions

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


# COMMAND ----------

circuit_df = spark.read.parquet(f"/mnt/formulaone/silver/circuit/{current_dt}/")
circuit_df.display()

# COMMAND ----------

constructors_df = spark.read.parquet(f"/mnt/formulaone/silver/constructors/{current_dt}/")
constructors_df.display()

# COMMAND ----------

pitstops_df = spark.read.parquet(f"/mnt/formulaone/silver/pitstops/{current_dt}/")
pitstops_df.display()

# COMMAND ----------

laptimes_df = spark.read.parquet(f"/mnt/formulaone/silver/laptimes/{current_dt}/")
laptimes_df.display()
