# Databricks notebook source
# MAGIC %run "./includes"

# COMMAND ----------

df=spark.read.json(f"{input_path_formula1}results.json",multiLine=True)

# COMMAND ----------

df.write.parquet(f"{processed_path_formula1}dixit/results",mode="overwrite")

# COMMAND ----------

a=dbutils.fs.ls(input_path_formula1)

# COMMAND ----------

type(a)

# COMMAND ----------

for type in a:
    print(type.name)

# COMMAND ----------

for name in [file.name for file in dbutils.fs.ls(input_path_formula1)]:
    if ".json" in name:
        df=spark.read.json(f"{input_path_formula1}{name}",multiLine=True)
    elif ".csv" in name:
        df=spark.read.csv(f"{input_path_formula1}{name}",header=True,inferSchema=True)
    elif "/" in name:
        if ".json" in name:
            df=spark.read.json(f"{input_path_formula1}{name}",multiLine=True)
        elif ".csv" in name:
            df=spark.read.csv(f"{input_path_formula1}{name}",header=True,inferSchema=True)

# COMMAND ----------

listPath=[]
def getFileNames(path):
    return 

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/formula1_raw/lap_times/

# COMMAND ----------


