# Databricks notebook source
# MAGIC %run "../Day 1/includes"

# COMMAND ----------

df=spark.read.csv(f"{input_path_formula1}circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=(df.withColumnRenamed("circuitId","circuit_id")
      .withColumnRenamed("circuitRef","circuit_Ref")
      .withColumn("ingestion_id",current_timestamp())
      .drop("url"))

# COMMAND ----------

df1.write.saveAsTable("dixit.circuit")

# COMMAND ----------

df1.write.parquet(f"{processed_path_formula1}circuit")

# COMMAND ----------

display(spark.read.parquet(f"{processed_path_formula1}circuit"))

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.fsutils.mv("dbfs:/FileStore/tables/processedformula1/dixit/circuit","dbfs:/FileStore/tables/processedformual1/dixit/circuit",True)

# COMMAND ----------


