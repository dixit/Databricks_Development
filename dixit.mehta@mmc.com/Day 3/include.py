# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

input_stream="dbfs:/mnt/cloudthats3/stream/input/csv_raw/emp"
output_stream="dbfs:/mnt/cloudthats3/stream/output/"
input_formula1="dbfs:/FileStore/tables/formula1/"

# COMMAND ----------

def addIngestionColumn(df):
    return df.withColumn("ingestion_id",current_timestamp())

# COMMAND ----------

dbutils.widgets.text("catalog_schema","hive_metastore.dixit")
catalog_schema=dbutils.widgets.get("catalog_schema")

# COMMAND ----------

dbutils.widgets.text("catalog_schema","hive_metastore.dixit")

