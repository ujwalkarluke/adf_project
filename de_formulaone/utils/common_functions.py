# Databricks notebook source
from pyspark.sql.types import ( 
                               StructField, 
                               StructType, 
                               IntegerType, 
                               StringType, 
                               FloatType,
                               DateType,
                                )

from datetime import datetime
from pyspark.sql.functions import lit

# COMMAND ----------

def create_csv_df(input_location, schema):
    """
    This function is used for creating a spark dataframe on csv file location
    :input_location: provide input csv file
    :schema: provide input schema
    :return : return dataframe
    """
    return spark.read.csv(input_location, header = True, schema= schema)
