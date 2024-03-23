# Databricks notebook source
countries = [("USA", 10000, 20000), ("India", 1000, 1500), ("UK", 7000, 10000), ("Canada", 500, 700) ]
columns = ["Country","NumVaccinated","AvailableDoses"]
spark.createDataFrame(data=countries, schema = columns).write.format("delta").mode("overwrite").saveAsTable("dixit.silverTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dixit.silverTable;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.read.table("dixit.silverTable").withColumn("VaccinationRate",col("NumVaccinated")/col("AvailableDoses")).drop("NumVaccinated","AvailableDoses").write.mode("overwrite").saveAsTable("dixit.goldTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dixit.goldTable

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dixit.silverTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# Insert new records
new_countries = [("Australia", 100, 3000)]
columns = ["Country","NumVaccinated","AvailableDoses"]
spark.createDataFrame(data=new_countries, schema = columns).write.format("delta").mode("append").saveAsTable("dixit.silverTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- update a record
# MAGIC UPDATE dixit.silverTable SET NumVaccinated = '11000' WHERE Country = 'USA';
# MAGIC DELETE from dixit.silverTable WHERE Country = 'UK';

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history dixit.silverTable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes("dixit.silverTable",3,5)

# COMMAND ----------

changes_df = spark.read.option("readChangeData", True).option("startingVersion", 2).table('dixit.silverTable')
display(changes_df)

# COMMAND ----------

changes_df = spark.read.option("readChangeData", True).option("startingVersion", 2).table('dixit.silverTable')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Collect only the latest version for each country
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silverTable_latest_version as
# MAGIC SELECT * 
# MAGIC     FROM 
# MAGIC          (SELECT *, rank() over (partition by Country order by _commit_version desc) as rank
# MAGIC           FROM table_changes('dixit.silverTable', 3,5)
# MAGIC           WHERE _change_type !='update_preimage')
# MAGIC     WHERE rank=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverTable_latest_version;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge the changes to gold
# MAGIC MERGE INTO dixit.goldTable t USING silverTable_latest_version s ON s.Country = t.Country
# MAGIC         WHEN MATCHED AND s._change_type='update_postimage' THEN UPDATE SET VaccinationRate = s.NumVaccinated/s.AvailableDoses
# MAGIC         WHEN MATCHED AND s._change_type='delete' THEN DELETE
# MAGIC         WHEN NOT MATCHED THEN INSERT (Country, VaccinationRate) VALUES (s.Country, s.NumVaccinated/s.AvailableDoses)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dixit.goldTable

# COMMAND ----------


