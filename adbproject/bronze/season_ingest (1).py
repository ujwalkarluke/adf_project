# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

# create table seasons

from pyspark.sql.functions import col,explode
df = spark.read.option("multiline", "true").json('/mnt/saf1racing/formulaoneproject/bronze/seasons/2024-04-11')
df = df.withColumn('list',explode(col('MRData').SeasonTable.Seasons))
df = df.drop('MRData')
df = df.withColumn('season',col('list').season).withColumn('url',col('list').url)
df = df.drop('list')
df.display()

# COMMAND ----------

# create table race

from pyspark.sql.functions import col,explode
df = spark.read.json('/mnt/saf1racing/formulaoneproject/bronze/races/2024-04-11')
df = df.withColumn('list',explode(col('MRData').RaceTable.Races))
df = df.drop('MRData')
df = df.withColumn('season',col('list').season).withColumn('round',col('list').round).withColumn('date',col('list').date)\
    .withColumn('race_name',col('list').raceName).withColumn('circuit_id',col('list').Circuit.circuitId)
df = df.drop('list')
df.display()

# COMMAND ----------

file_paths= []
root_path = '/mnt/saf1racing/formulaoneproject/bronze/results'
l = dbutils.fs.ls('/mnt/saf1racing/formulaoneproject/bronze/results')
subdirectories = []
for i in l:
    if i.isDir():
        subdirectories.append(dbutils.fs.ls(i[0]))
print(subdirectories)

        
    # while i[0].endswith('/'):
    #     print(i[0])
    #     print(dbutils.fs.ls(i[0]))

    # print(i)
    # if i[0].endswith('/'):
    #     print(i[0])
# dbutils.fs.ls('/mnt/saf1racing/formulaoneproject/bronze/results')





# COMMAND ----------


# from pyspark.sql.functions import col,explode
# df = spark.read.json('/mnt/saf1racing/formulaoneproject/bronze/races/2024-04-11')
# df = df.withColumn('list',explode(col('MRData').RaceTable.Races))
# df = df.drop('MRData')
# df = df.withColumn('season',col('list').season).withColumn('round',col('list').round).withColumn('date',col('list').date)\
#     .withColumn('race_name',col('list').raceName).withColumn('circuit_id',col('list').Circuit.circuitId)
# df = df.drop('list')
# df.display()
