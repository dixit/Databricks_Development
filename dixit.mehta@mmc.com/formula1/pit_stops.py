# Databricks notebook source
# MAGIC %run "./includes"

# COMMAND ----------

df=spark.read.json(f"{input_path_formula1}pit_stops.json",multiLine=True)
df1=df.withColumn("ingestion_id",current_timestamp())

# COMMAND ----------

df1.write.parquet(f"{processed_path_formula1}dixit/pit_stops",mode="overwrite")
