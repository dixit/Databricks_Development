# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/raw/complexjson.json",multiLine=True)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %run "../Day 1/includes"

# COMMAND ----------

df1=(df.withColumn("topping",explode("topping"))
     .withColumn("topping_id",col("topping.id"))
     .withColumn("topping_type",col("topping.type"))
   .drop("topping")
   .withColumn("batters",explode("batters.batter"))
      .withColumn("batter_id",col("batters.id"))
      .withColumn("batter_type",col("batters.type"))
   .drop("batters"))

# COMMAND ----------

df1.write.saveAsTable("dixit.netedData")

# COMMAND ----------


