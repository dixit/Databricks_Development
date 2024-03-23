# Databricks notebook source
# MAGIC %run "../Day 1/includes"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

{"driverId":1,"driverRef":"hamilton","number":44,"code":"HAM","name":{"forename":"Lewis","surname":"Hamilton"},"dob":"1985-01-07","nationality":"British","url":"http://en.wikipedia.org/wiki/Lewis_Hamilton"}

# COMMAND ----------

driverSchema=StructType([StructField("driverId",IntegerType()),
                         StructField("driverRef",StringType()),
                         StructField("number",IntegerType()),
                         StructField("code",StringType()),
                         StructField("name",StructType([StructField("forename",StringType()),
                                                        StructField("surname",StringType())])),
                         StructField("dob",DateType()),
                         StructField("nationality",StringType()),
                         StructField("url",StringType())])

# COMMAND ----------

df=spark.read.schema(driverSchema).json(f"{input_path_formula1}drivers.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df1=(df.withColumn("forename",df.name.forename)
       .withColumn("surname",df.name.surname)
       .withColumn("ingestion_date",current_timestamp())
       .drop("name"))

# COMMAND ----------

display(df1)

# COMMAND ----------

jsonPath=f"{input_path_formula1}drivers.json"
print(jsonPath)
spark.sql(f"select name.forename from json.`{jsonPath}`").display()

# COMMAND ----------

df1.write.saveAsTable("dixit.drivers")

# COMMAND ----------

df1.write.mode("overwrite").option("mrgerSchema","true").saveAsTable("dixit.drivers")

# COMMAND ----------


