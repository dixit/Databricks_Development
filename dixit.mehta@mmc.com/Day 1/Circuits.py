# Databricks notebook source
# MAGIC %run "/Workspace/Users/dixit.mehta@mmc.com/Day 1/includes"

# COMMAND ----------

df=spark.read.csv(f"{input_path_formula1}circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df.write.saveAsTable("hive_metastore.dixit.formula1")

# COMMAND ----------

df.display()

# COMMAND ----------

df.select("circuitId","name")

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("circuitId").alias("circuit_id"),"name").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(concat("location",lit(" & "),"country").alias("location&country")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------

df.columns
