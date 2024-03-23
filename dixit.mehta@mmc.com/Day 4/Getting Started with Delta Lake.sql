-- Databricks notebook source
CREATE TABLE dixit.people10m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
) LOCATION 'dbfs:/mnt/cloudthats3/delta_lake/dixit/people10m'

-- COMMAND ----------

describe extended dixit.people10m

-- COMMAND ----------

INSERT INTO dixit.people10m VALUES(1,'Dixit','B','Mehta',"MALE",'2024-03-21T00:00:0',"1234",1000)

-- COMMAND ----------

select * from dixit.people10m

-- COMMAND ----------

INSERT INTO dixit.people10m VALUES(2,'A','B','c',"FEMALE",'2024-03-21T00:00:0',"1234",1000),
                                   (3,'B','B','c',"MALE",'2024-03-21T00:00:0',"1234",1000)

-- COMMAND ----------

DELETE FROM dixit.people10m WHERE ID=3

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/mnt/cloudthats3/delta_lake/dixit/people10m/_delta_log/00000000000000000001.json

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/cloudthats3/delta_lake/dixit/people10m/_delta_log/

-- COMMAND ----------

UPDATE dixit.people10m SET gender="MALE" where id=2;
INSERT INTO dixit.people10m VALUES(3,'B','B','c',"MALE",'2024-03-21T00:00:0',"1234",1000);

-- COMMAND ----------

select * from dixit.people10m timestamp as of '2024-03-21T05:29:51Z'

-- COMMAND ----------

describe history dixit.people10m

-- COMMAND ----------

delete from dixit.people10m

-- COMMAND ----------

restore table dixit.people10m to version as of 5

-- COMMAND ----------

select * from dixit.people10m

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.parquet("dbfs:/mnt/cloudthats3/delta_lake/dixit/people10m/_delta_log/00000000000000000007.checkpoint.parquet")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

optimize dixit.people10m
zorder by (id)

-- COMMAND ----------

describe history dixit.people10m

-- COMMAND ----------

vacuum dixit.people10m

-- COMMAND ----------

select * from dixit.people10m version as of 3

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vaccum.logging.enabled = True;

-- COMMAND ----------

vacuum dixit.people10m retain 0 hours dry run

-- COMMAND ----------

vacuum dixit.people10m retain 0 hours

-- COMMAND ----------

select * from dixit.people10m version as of 2

-- COMMAND ----------

vacuum dixit.people10m retain 0 hours dry run

-- COMMAND ----------

describe extended dixit.people10m

-- COMMAND ----------


