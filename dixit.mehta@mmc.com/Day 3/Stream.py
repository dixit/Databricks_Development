# Databricks notebook source
# MAGIC %run "./include"

# COMMAND ----------

user_stream= "Id int, Name string, Gender string, Salary int, Country string, Date string"

# COMMAND ----------

(spark
 .readStream
 .schema(user_stream)
 .csv(f"{input_stream}",header=True)
 .writeStream
 .option("checkpointLocation",f"{output_stream}dixit/emp/checkpoint")
 .trigger(once=True)
 .toTable("dixit.firststream"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.dixit.firststream

# COMMAND ----------


