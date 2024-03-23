-- Databricks notebook source
-- MAGIC %python
-- MAGIC input_path="dbfs:/mnt/cloudthats3/stream/input/csv_raw/finance_data/"
-- MAGIC output_path="dbfs:/mnt/cloudthats3/stream/output"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=(spark.readStream
-- MAGIC      .format("cloudFiles")
-- MAGIC      .option("cloudFiles.format","csv")
-- MAGIC      .option("cloudFiles.schemaLocation",f"{output_path}/dixit/finance_autoloader/schemalocation")
-- MAGIC      .option("cloudFiles.inferColumnTypes",True)
-- MAGIC      .option("cloudFiles.schemaEvolutionMode","failOnRes")
-- MAGIC      .load(f"{input_path}")
-- MAGIC      .writeStream
-- MAGIC      .option("checkpointLocation",f"{output_path}/dixit/finance_autoloader/checkpoint")
-- MAGIC      .option("mergeSchema",True)
-- MAGIC      .trigger(once=True)
-- MAGIC      .table("dixit.finance_autoloader"))
