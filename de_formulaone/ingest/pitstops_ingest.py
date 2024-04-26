# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

# Define schema
input_schema = StructType ([
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('stop', IntegerType()),
    StructField('lap', IntegerType()),
    StructField('time', StringType()),
    StructField('duration', StringType()),
    StructField('milliseconds', IntegerType())
])

# COMMAND ----------

display(dbutils.fs.ls('/mnt/bronze/'))

# COMMAND ----------

df = spark.read.json('/mnt/bronze/pit_stops.json',schema= input_schema, multiLine=True).display()

# COMMAND ----------

df.display()

# COMMAND ----------


