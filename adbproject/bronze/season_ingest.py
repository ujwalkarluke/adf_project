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
input_file_path = f"/mnt/formulaone/bronze/seasons/{current_dt}"
output_file_path = f"/mnt/formulaone/silver/seasons/{current_dt}"

# COMMAND ----------

from pyspark.sql.functions import col,explode

# COMMAND ----------

df = spark.read.json(input_file_path, multiLine=True)

# COMMAND ----------

df.columns

# COMMAND ----------

df = df.withColumn('list',explode(col('MRData').SeasonTable.Seasons))


# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop('MRData')

# COMMAND ----------

df = df.withColumn('season',col('list').season).withColumn('url',col('list').url)

# COMMAND ----------

df = df.drop('list')

# COMMAND ----------

df.display()

# COMMAND ----------

df.distinct().write.mode('overwrite').parquet(output_file_path)
