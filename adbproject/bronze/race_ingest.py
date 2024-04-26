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
input_file_path = f"/mnt/formulaone/bronze/races/{current_dt}"
output_file_path = f"/mnt/formulaone/silver/races/{current_dt}"

# COMMAND ----------

from pyspark.sql.functions import col,explode

# COMMAND ----------

df = spark.read.json(input_file_path)

# COMMAND ----------

df.display()

# COMMAND ----------

df.columns

# COMMAND ----------

df = df.withColumn('list',explode(col('MRData').RaceTable.Races))


# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop('MRData')

# COMMAND ----------

df = df.withColumn('season',col('list').season).withColumn('round',col('list').round).withColumn('circuit_id',col('list').Circuit.circuitId).withColumn('date',col('list').date).withColumn('Race_name',col('list').raceName)#.withColumn('Race_date',col('list').racetime)

# COMMAND ----------

df = df.drop('list')

# COMMAND ----------

df.display()

# COMMAND ----------

#df.write.mode('overwrite').format('delta').save(output_file_path)
df.distinct().write.mode('overwrite').parquet(output_file_path)
