# Databricks notebook source
# MAGIC %run "./include"

# COMMAND ----------

dbutils.widgets.text("catalog_schemas","hive_metastore.dixit")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${catalog_schemas}.circuit_final where environment='${environ}'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${catalog_schemas}.circuit_final where environment=variable_para'

# COMMAND ----------

df1=spark.read.csv(f"{input_formula1}circuits.csv",header=True,inferSchema=True)
df1=addIngestionColumn(df1)

# COMMAND ----------

dbutils.widgets.text("environ","")
variable_para=dbutils.widgets.get("environ")

# COMMAND ----------

df1=df1.withColumn("environment",lit(variable_para))
df1.write.saveAsTable("dixit.circuit_final",mode="overwrite")

# COMMAND ----------


