-- Databricks notebook source
-- MAGIC %python
-- MAGIC input_path="dbfs:/mnt/cloudthats3/stream/input/csv_raw/emp_auto"
-- MAGIC output_path="dbfs:/mnt/cloudthats3/stream/output"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=(spark.readStream
-- MAGIC      .format("cloudFiles")
-- MAGIC      .option("cloudFiles.format","csv")
-- MAGIC      .option("cloudFiles.schemaLocation",f"{output_path}/dixit/emp_autoloader/schemalocation")
-- MAGIC      .load(f"{input_path}")
-- MAGIC      .writeStream
-- MAGIC      .option("checkpointLocation",f"{output_path}/dixit/emp_autoloader/checkpoint")
-- MAGIC      .table("dixit.emp_autoloader"))

-- COMMAND ----------

select * from dixit.emp_autoloader;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=(spark.readStream
-- MAGIC      .format("cloudFiles")
-- MAGIC      .option("cloudFiles.format","csv")
-- MAGIC      .option("cloudFiles.schemaLocation",f"{output_path}/dixit/emp_autoloader/schemalocation")
-- MAGIC      .option("cloudFiles.inferColumnTypes",True)
-- MAGIC      .load(f"{input_path}")
-- MAGIC      .writeStream
-- MAGIC      .option("checkpointLocation",f"{output_path}/dixit/emp_autoloader/checkpoint")
-- MAGIC      .trigger(once=True)
-- MAGIC      .table("dixit.emp_autoloader"))

-- COMMAND ----------

drop table dixit.emp_autoloader

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.table("dixit.emp_autoloader")

-- COMMAND ----------

select * from dixit.emp_autoloader;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=(spark.readStream
-- MAGIC      .format("cloudFiles")
-- MAGIC      .option("cloudFiles.format","csv")
-- MAGIC      .option("cloudFiles.schemaLocation",f"{output_path}/dixit/emp_autoloader/schemalocation")
-- MAGIC      .option("cloudFiles.inferColumnTypes",True)
-- MAGIC      .load(f"{input_path}")
-- MAGIC      .writeStream
-- MAGIC      .option("checkpointLocation",f"{output_path}/dixit/emp_autoloader/checkpoint")
-- MAGIC      .trigger(once=True)
-- MAGIC      .table("dixit.emp_autoloader"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=(spark.readStream
-- MAGIC      .format("cloudFiles")
-- MAGIC      .option("cloudFiles.format","csv")
-- MAGIC      .option("cloudFiles.schemaLocation",f"{output_path}/dixit/emp_autoloader/schemalocation")
-- MAGIC      .option("cloudFiles.inferColumnTypes",True)
-- MAGIC      .load(f"{input_path}")
-- MAGIC      .writeStream
-- MAGIC      .option("checkpointLocation",f"{output_path}/dixit/emp_autoloader/checkpoint")
-- MAGIC      .trigger(once=True)
-- MAGIC      .table("dixit.emp_autoloader"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=(spark.readStream
-- MAGIC      .format("cloudFiles")
-- MAGIC      .option("cloudFiles.format","csv")
-- MAGIC      .option("cloudFiles.schemaLocation",f"{output_path}/dixit/emp_autoloader/schemalocation")
-- MAGIC      .option("cloudFiles.inferColumnTypes",True)
-- MAGIC      .load(f"{input_path}"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=(spark.readStream
-- MAGIC      .format("cloudFiles")
-- MAGIC      .option("cloudFiles.format","csv")
-- MAGIC      .option("cloudFiles.schemaLocation",f"{output_path}/dixit/emp_autoloader/schemalocation")
-- MAGIC      .option("cloudFiles.inferColumnTypes",True)
-- MAGIC      .load(f"{input_path}")
-- MAGIC      .writeStream
-- MAGIC      .option("checkpointLocation",f"{output_path}/dixit/emp_autoloader/checkpoint")
-- MAGIC      .option("mergeSchema",True)
-- MAGIC      .trigger(once=True)
-- MAGIC      .table("dixit.emp_autoloader"))

-- COMMAND ----------

select * from dixit.emp_autoloader;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC help(df.writeStream)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=(spark.readStream
-- MAGIC      .format("cloudFiles")
-- MAGIC      .option("cloudFiles.format","csv")
-- MAGIC      .option("cloudFiles.schemaLocation",f"{output_path}/dixit/emp_autoloader/schemalocation")
-- MAGIC      .option("cloudFiles.inferColumnTypes",True)
-- MAGIC      .option("cloudFiles.schemaEvolutionMode","failOnNewColumns")
-- MAGIC      .load(f"{input_path}")
-- MAGIC      .writeStream
-- MAGIC      .option("checkpointLocation",f"{output_path}/dixit/emp_autoloader/checkpoint")
-- MAGIC      .option("mergeSchema",True)
-- MAGIC      .trigger(once=True)
-- MAGIC      .table("dixit.emp_autoloader"))

-- COMMAND ----------

select * from dixit.emp_autoloader;

-- COMMAND ----------


