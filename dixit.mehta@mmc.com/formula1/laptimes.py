# Databricks notebook source
# MAGIC %run "./includes"

# COMMAND ----------

schema = StructType([
    StructField("raceId", IntegerType()),
    StructField("driverID", IntegerType()),
    StructField("lap", IntegerType()),
    StructField("position", IntegerType()),
    StructField("time", StringType()),
    StructField("milliseconds", IntegerType())
])
df_laptimes=spark.read.csv(f"{input_path_formula1}lap_times", header=True, schema=schema)

# COMMAND ----------

df1=df_laptimes.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

df1.write.parquet(f"{processed_path_formula1}dixit/lap_times")
