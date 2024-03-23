# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM dixit.bronze where key like 'C%'
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select schema_of_json('{"customer_id":"C01479","first_name":"Michel","last_name":"Sterricks","gender":"Female","city":"Qiqian","country_code":"CN","row_status":"insert","row_time":"2022-11-03T15:04:00.000Z"}') as schemaCusomters

# COMMAND ----------

# MAGIC %sql
# MAGIC select schema_of_json('{"book_id":"B14","title":"Data Communications and Networking","author":"Behrouz A. Forouzan","price":34,"updated":"2021-11-11 17:12:42.335"}') as schemaCusomters

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select schema_of_json('{"order_id":"000000005234","order_timestamp":"2022-04-01 00:09:00","customer_id":"C00558","quantity":2,"total":77,"books":[{"book_id":"B02","quantity":1,"subtotal":28},{"book_id":"B01","quantity":1,"subtotal":49}]}') as schemaOrders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "city STRING, country_code STRING, customer_id STRING, first_name STRING, gender STRING, last_name STRING, row_status STRING, row_time STRING") v
# MAGIC   FROM dixit.bronze
# MAGIC   WHERE topic = "customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "author STRING, book_id STRING, price BIGINT, title STRING, updated STRING") v
# MAGIC   FROM dixit.bronze
# MAGIC   WHERE topic = "books")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "books ARRAY<STRUCT<book_id: STRING, quantity BIGINT, subtotal BIGINT>>, customer_id STRING, order_id STRING, order_timestamp STRING, quantity BIGINT, total BIGINT") v
# MAGIC   FROM dixit.bronze
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

(spark.readStream
      .table("dixit.bronze")
      .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customers_silver_tmp AS
# MAGIC   SELECT v.*
# MAGIC   FROM (SELECT from_json(cast(value AS STRING), "city STRING, country_code STRING, customer_id STRING,  
# MAGIC          first_name STRING, gender STRING, last_name STRING, row_status STRING, row_time STRING") v
# MAGIC         FROM dixit.bronze
# MAGIC         WHERE topic = "customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "books ARRAY<STRUCT<book_id: STRING, quantity BIGINT, subtotal BIGINT>>, customer_id STRING, order_id STRING, order_timestamp STRING, quantity BIGINT, total BIGINT") v
# MAGIC     FROM dixit.bronze
# MAGIC     WHERE topic = "orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW books_silver_tmp AS
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "author STRING, book_id STRING, price BIGINT, title STRING, updated STRING") v
# MAGIC     FROM dixit.bronze
# MAGIC     WHERE topic = "books")

# COMMAND ----------

query = (spark.table("orders_silver_tmp")
               .writeStream
               .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/dixit/orders_silver")
               .trigger(availableNow=True)
               .table("dixit.orders_silver"))

query.awaitTermination()

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()
