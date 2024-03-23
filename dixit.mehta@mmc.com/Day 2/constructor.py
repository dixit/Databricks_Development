# Databricks notebook source
# MAGIC %run "../Day 1/includes"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df=(spark.read.json(f"{input_path_formula1}constructors.json")
    .withColumnRenamed("constructorid","constructor_id")
    .withColumn("ingestionDate",current_timestamp())
    .drop("url"))
    

# COMMAND ----------

df.write.saveAsTable("dixit.constructor")

# COMMAND ----------

df.write.parquet("")
