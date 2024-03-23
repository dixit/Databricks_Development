# Databricks notebook source
# MAGIC %run "./includes"

# COMMAND ----------

df=spark.read.json(f"{input_path_formula1}constructors.json",multiLine=True)
df1=df.withColumn("ingestion_id",current_timestamp())

# COMMAND ----------

df1.write.parquet(f"{processed_path_formula1}dixit/constructors",mode="overwrite")
