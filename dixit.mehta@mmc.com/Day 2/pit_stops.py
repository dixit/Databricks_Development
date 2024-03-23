# Databricks notebook source
# MAGIC %run "../Day 1/includes"

# COMMAND ----------

df=spark.read.json(f"{input_path_formula1}pit_stops.json",multiLine=True)

# COMMAND ----------

df.write.saveAsTable("dixit.pit_stop")

# COMMAND ----------


