# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Start

# COMMAND ----------

print ("First Python Code!!")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create schema dixit;
# MAGIC use schema dixit;

# COMMAND ----------

data=([(1,'a',30),(2,'b',34)])
schema="id int, name string, age int"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/Proc")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/tables/raw/emp.csv","dbfs:/FileStore/Proc")

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/raw/emp.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/raw/emp.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()
df.printSchema()

# COMMAND ----------

df.write.saveAsTable("dixit.emp_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,dept,max(age) partition by() from hive_metastore.dixit.emp_demo

# COMMAND ----------


