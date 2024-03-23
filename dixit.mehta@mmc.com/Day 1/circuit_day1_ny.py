# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/formula1/

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

dataframe function

1.select 
2. alias
3. withColumnRenamed
4. toDF
5. withColumn
6. drop



functions
1. col
2. concat
3. lit
4. current_date

# COMMAND ----------

df.select("circuitId","name")

# COMMAND ----------

spark is lazy evaluated
T & A

# COMMAND ----------

df.select("circuitId","name").display()

# COMMAND ----------

df.select("circuitId".alias("circuit_id"),"name").display()

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"),"name").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select("*",col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"),"name").display()

# COMMAND ----------

df.select("circuitId",col("name"),df.location,df["country"]).display()

# COMMAND ----------

df.select(concat("location"," ","country")).display()

# COMMAND ----------

df.select(concat("location",lit(" & "),"country").alias("location&county")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").display()

# COMMAND ----------

df.columns

# COMMAND ----------

new_column=['circuit_id',
 'circuit_ref',
 'name',
 'location',
 'country',
 'latitude',
 'longitute',
 'altitude',
 'url']

# COMMAND ----------

df1=df.toDF(*new_column)

# COMMAND ----------

df2=df1.drop("url")

# COMMAND ----------

help(withColumn)

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

help(lit)

# COMMAND ----------

df2.withColumn("ingestion_date",current_date()).display()

# COMMAND ----------

df2.withColumn("newcolumn",lit("formula1")).display()

# COMMAND ----------

df2\
.withColumn("location&country",concat("location", lit(" "),"country"))\
.drop("country","location")\
.display()

# COMMAND ----------

(df2
.withColumn("location&country",concat("location", lit(" "),"country"))
.drop("country","location")
.display())

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.withColumn("country",upper("country")).display()

# COMMAND ----------

Task: 
    to get a new column with current timestamp: column name should be ingetion_date

# COMMAND ----------

df2.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

help(df2.filter)

# COMMAND ----------

df2.where("location='Melbourne' ").display()

# COMMAND ----------

df2.where(col("circuit_id")>5).display()

# COMMAND ----------


