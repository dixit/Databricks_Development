# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

input_path_formula1="dbfs:/mnt/cloudthats3/formula1_raw/"
processed_path_formula1="dbfs:/mnt/cloudthats3/formula1_processed_parquet/"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/
