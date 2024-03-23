# Databricks notebook source
df=spark.read.csv("dbfs:/mnt/sanly/raw/jsoninputfiles/races.csv",header=True,inferSchema=True)

# COMMAND ----------

df.write.parquet("dbfs:/mnt/sanly/raw/processed/dixit/races")

# COMMAND ----------


