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
input_file_path = f"/mnt/formulaone/bronze/drivers/{current_dt}"
output_file_path = f"/mnt/formulaone/silver/drivers/{current_dt}"

# COMMAND ----------

from pyspark.sql.functions import col,explode

# COMMAND ----------

df = spark.read.json(input_file_path, multiLine=True)

# COMMAND ----------

df.columns

# COMMAND ----------

df = df.withColumn('list',explode(col('MRData').DriverTable.Drivers))


# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop('MRData')

# COMMAND ----------

df = df.withColumn('driver_id',col('list').driverId).withColumn('forename',col('list').givenName).withColumn('surname',col('list').familyName).withColumn('nationality',col('list').nationality).withColumn('dob',col('list').dateOfBirth).withColumn('number',col('list').permanentNumber).withColumn('url',col('list').url)

# COMMAND ----------

df = df.drop('list')

# COMMAND ----------

df.display()

# COMMAND ----------

df.distinct().write.mode('overwrite').parquet(output_file_path)

# COMMAND ----------


df = spark.read.option("multiline", "true").json('/mnt/saf1racing/formulaoneproject/bronze/seasons/2024-04-11')
df = df.withColumn('list',explode(col('MRData').SeasonTable.Seasons))
df = df.drop('MRData')
df = df.withColumn('season',col('list').season).withColumn('url',col('list').url)
df = df.drop('list')
df.display()
