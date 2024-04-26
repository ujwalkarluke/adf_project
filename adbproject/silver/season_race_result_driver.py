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
input_file_path = "/mnt/formulaone/silver/"
output_file_path = "/mnt/formulaone/gold/"

# COMMAND ----------

season_df = spark.read.parquet(f"{input_file_path}seasons/{current_dt}/")
season_df.display()

# COMMAND ----------

races_df = spark.read.parquet(f"{input_file_path}races/{current_dt}/")
races_df.display()

# COMMAND ----------

results_df = spark.read.parquet(f"{input_file_path}results/20240412/")
results_df.display()
#results_df.write.mode("overwrite").parquet(f"{output_file_path}results/{current_dt}/")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{input_file_path}drivers/{current_dt}/")
#drivers_df.drop(col("url"))
drivers_df.display()
#drivers_df.write.mode("overwrite").parquet(f"{output_file_path}drivers/{current_dt}/")
