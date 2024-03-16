# Databricks notebook source
# MAGIC %run "/Workspace/Users/dixit.mehta@mmc.com/Day 1/includes"

# COMMAND ----------

df=spark.read.json(f"{input_path}iot1.json")

# COMMAND ----------

df.write.saveAsTable("hive_metastore.dixit.iot")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table hive_metastore.dixit.iot;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.dixit.iot;

# COMMAND ----------

help(spark.read.json)

# COMMAND ----------


