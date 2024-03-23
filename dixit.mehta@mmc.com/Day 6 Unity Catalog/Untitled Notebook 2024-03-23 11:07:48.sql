-- Databricks notebook source
select * from json.`/Volumes/mmc_dev/documents/ppt/course.json`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.json("/Volumes/mmc_dev/documents/ppt/course.json",multiLine=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.display()

-- COMMAND ----------


