# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

input_path="dbfs:/FileStore/tables/raw/"
input_path_formula1="dbfs:/FileStore/tables/formula1/"
processed_path_formula1="dbfs:/FileStore/tables/processedformula1/dixit/"

# COMMAND ----------


