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
input_file_path = f"/mnt/formulaone/bronze/results/{current_dt}"
output_file_path = f"/mnt/formulaone/silver/results/{current_dt}"

# COMMAND ----------

from pyspark.sql.functions import col,explode

# COMMAND ----------

# Define schema

input_schema = StructType(
    [
        StructField("constructorId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("fastestLap", IntegerType()),
        StructField("fastestLapSpeed", FloatType()),
        StructField("fastestLapTime", StringType()),
        StructField("grid", IntegerType()),
        StructField("laps", IntegerType()),
        StructField("milliseconds", IntegerType()),
        StructField("number", IntegerType()),
        StructField("points", FloatType()),
        StructField("position", IntegerType()),
        StructField("positionOrder", IntegerType()),
        StructField("positionText", StringType()),
        StructField("raceId", IntegerType()),
        StructField("rank", IntegerType()),
        StructField("resultId", IntegerType()),
        StructField("statusId", IntegerType()),
        StructField("time", StringType()),

    ]
)

# COMMAND ----------

df = spark.read.json(input_file_path)#schema=input_schema)#


# COMMAND ----------

df.display()

# COMMAND ----------

df1= df.select("MRData.RaceTable.Races.season","MRData.RaceTable.Races.round", "MRData.RaceTable.Races.Circuit.CircuitId", "MRData.RaceTable.Races.Results")
df1.display()

# COMMAND ----------

df1 = df1.withColumn("season", explode("season")).withColumn("round", explode("round")).withColumn("circuitid", explode("circuitId")).withColumn("Results", explode("Results")).withColumn("Results", explode("Results"))
df1.display()

# COMMAND ----------

final_df = df1.select("season","round", "circuitId", "Results.Constructor.ConstructorId", "Results.Driver.driverId", col("Results.FastestLap.Time.time").alias("FastestLapTime"),"Results.grid", "Results.laps", "Results.position", "Results.points", "Results.positionText",)

# COMMAND ----------

final_df.display()

# COMMAND ----------

final_df.distinct().repartition(1).write.mode("overwrite").parquet(output_file_path)

# COMMAND ----------

#       .withColumn('constructor_id',col('list').Results.Constructor.constructorId)\
#        .withColumn('driver_id',col('list').Results.Driver.driverId)\
#        .withColumn('grid',col('list').Results.grid)\
#        .withColumn('laps',col('list').Results.laps)\
#        .withColumn('position',col('list').Results.position)\
#        .withColumn('points',col('list').Results.points)\
#        .withColumn('positiontext',col('list').Results.positionText)\
#        .withColumn('FastestLapTime',col('list').Results.FastestLap.Time.time)\


