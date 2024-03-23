# Databricks notebook source
df=spark.read.table("naval.firstStream")

# COMMAND ----------

df1=df.dropDuplicates(["id"]).dropna(how="any",subset="id")

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("naval.stream_silver")
