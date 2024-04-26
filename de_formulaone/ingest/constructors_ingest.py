# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

#define current date value 'YYYY-MM-DD'
current_dt = datetime.today().strftime('%Y-%m-%d')

# COMMAND ----------

# Define schema

input_schema = StructType ([
    StructField('constructorsId', IntegerType()),
    StructField('constructorsRef', StringType()),
    StructField('name', StringType()),
    StructField('nationality', StringType()),
    StructField('url', StringType()),
])

# COMMAND ----------

display(dbutils.fs.ls('/mnt/bronze/'))

# COMMAND ----------

df = create_csv_df('/mnt/bronze/constructors.json', input_schema)

# COMMAND ----------

# rename and add a new column with current date
df = df.withColumnRenamed('constructorsId', 'constructors_id').withColumnRenamed('constructorsRef', 'constructors_ref').withColumn('ingest_dt',lit(current_dt))

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.write.parquet('/mnt/silver/constructors')

# COMMAND ----------

dbutils.fs.ls('/mnt/silver/constructors/')

# COMMAND ----------

display(spark.read.parquet('/mnt/silver/constructors/'))
