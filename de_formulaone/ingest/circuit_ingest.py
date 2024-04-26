# Databricks notebook source
# MAGIC %md
# MAGIC #####1. Read a csv file
# MAGIC #####2. Apply schema for it
# MAGIC #####3. rename & remove column based on requirement
# MAGIC

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------



#Define schema
input_schema = StructType ([
    StructField("circuitId", IntegerType()),
    StructField("circuitRef", StringType()),
    StructField("name", StringType()),
    StructField("location", StringType()),
    StructField("country", StringType()),
    StructField("lat", FloatType()),
    StructField("lng", FloatType()),
    StructField("alt", IntegerType()),
    StructField("url", StringType()),
])

# COMMAND ----------

spark.read.csv("/mnt/bronze/circuits.csv", header = True).display()

# COMMAND ----------

#df = spark.read.csv("/mnt/bronze/circuits.csv", header = True, schema= input_schema)
df = create_csv_df("/mnt/bronze/circuits.csv", input_schema)

# COMMAND ----------

df.display()

# COMMAND ----------

display(dbutils.fs.ls('/mnt'))

# COMMAND ----------

df.write.parquet("/mnt/silver/circuits")

# COMMAND ----------

display(dbutils.fs.ls('/mnt/silver/'))

# COMMAND ----------

display(spark.read.parquet('/mnt/silver/circuits'))

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

# rename a column
df = df.withColumnRenamed('circuitId', 'circuit_id')
df = df.withColumnRenamed('circuitRef', 'circuit_ref')

#define current date value 'YYYY-MM-DD'
current_dt = datetime.today().strftime('%Y-%m-%d')

# add new column
df = df.withColumn('ingest_dt', lit(current_dt))

# COMMAND ----------

df.display()
