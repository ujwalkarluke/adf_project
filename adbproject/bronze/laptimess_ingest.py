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
input_file_path = f"/mnt/formulaone/bronze/laptimes/{current_dt}"
output_file_path = f"/mnt/formulaone/silver/laptimes/{current_dt}"

# COMMAND ----------

from pyspark.sql.functions import col, explode

# COMMAND ----------


input_schema = StructType(
    [
        StructField("raceId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("lap", IntegerType()),
        StructField("position", IntegerType()),
        StructField("time", StringType()),
        StructField("milliseconds", IntegerType()),
        
    ]
)

# COMMAND ----------

df = spark.read.csv(input_file_path,schema=input_schema)

# COMMAND ----------

df.columns

# COMMAND ----------

df = df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

df.columns

# COMMAND ----------

df.distinct().write.mode("overwrite").parquet(output_file_path)
