# Databricks notebook source
# MAGIC %run "./includes"

# COMMAND ----------

df=spark.read.json(f"{input_path_formula1}drivers.json",multiLine=True)
df1=df.withColumn("ingestion_id",current_timestamp())

# COMMAND ----------

df1.write.parquet(f"{processed_path_formula1}dixit/drivers",mode="overwrite")

# COMMAND ----------


