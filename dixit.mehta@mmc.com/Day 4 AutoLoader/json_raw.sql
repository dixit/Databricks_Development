-- Databricks notebook source
-- MAGIC %python
-- MAGIC input_path="dbfs:/mnt/cloudthats3/stream/input/json_raw/"
-- MAGIC output_path="dbfs:/mnt/cloudthats3/stream/output"

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/cloudthats3/stream/input/json_raw/

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC head dbfs:/mnt/cloudthats3/stream/input/json_raw/1.json

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=(spark.readStream
-- MAGIC      .format("cloudFiles")
-- MAGIC      .option("cloudFiles.format","json")
-- MAGIC      .option("cloudFiles.schemaLocation",f"{output_path}/dixit/json_autoloader/schemalocation")
-- MAGIC      .option("cloudFiles.schemaEvolutionMode","rescue")
-- MAGIC      .load(f"{input_path}")
-- MAGIC      .writeStream
-- MAGIC      .option("checkpointLocation",f"{output_path}/dixit/json_autoloader/checkpoint")
-- MAGIC      .option("mergeSchema",True)
-- MAGIC      .trigger(availableNow=True)
-- MAGIC      .table("dixit.json_autoloader"))

-- COMMAND ----------

select * from dixit.json_autoloader

-- COMMAND ----------

drop table dixit.json_autoloader

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/cloudthats3/stream/output/dixit/json_autoloader/checkpoint/

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/mnt/cloudthats3/stream/output/dixit/json_autoloader/checkpoint/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.help()

-- COMMAND ----------


