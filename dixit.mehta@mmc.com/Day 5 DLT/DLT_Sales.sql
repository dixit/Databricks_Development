-- Databricks notebook source
CREATE STREAMING LIVE TABLE SALES_BRONZE
COMMENT "The sales table in bronze"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
AS
(SELECT *,current_timestamp() as ingestion_date,input_file_name() as source_path
 FROM cloud_files("dbfs:/mnt/cloudthats3/dlt/sales","csv",
                           map("cloudFiles.inferColumnTypes","True")))

-- COMMAND ----------

CREATE STREAMING LIVE TABLE SALES_SILVER(
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The sales table in silver"
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS
(SELECT DISTINCT order_id,customer_id,product_id,quantity,discount_amount,total_amount,order_date
 FROM STREAM(LIVE.SALES_BRONZE))

-- COMMAND ----------

CREATE STREAMING LIVE TABLE CUSTOMERS_BRONZE
COMMENT "The Customers table in bronze"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
AS
(SELECT *,current_timestamp() as ingestion_date,input_file_name() as source_path
 FROM cloud_files("dbfs:/mnt/cloudthats3/dlt/customers","csv",
                           map("cloudFiles.inferColumnTypes","True")))

-- COMMAND ----------

CREATE STREAMING LIVE TABLE PRODUCTS_BRONZE
COMMENT "The Products table in bronze"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
AS
(SELECT *,current_timestamp() as ingestion_date,input_file_name() as source_path
 FROM cloud_files("dbfs:/mnt/cloudthats3/dlt/products","csv",
                           map("cloudFiles.inferColumnTypes","True")))

-- COMMAND ----------

-- Create and populate the target table.
CREATE OR REFRESH STREAMING TABLE CUSTOMERS_SILVER;

APPLY CHANGES INTO
  live.CUSTOMERS_SILVER
FROM
  stream(LIVE.CUSTOMERS_BRONZE)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation,sequenceNum,_rescued_data,ingestion_date,source_path)
STORED AS
  SCD TYPE 2;


-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE PRODUCTS_SILVER;

APPLY CHANGES INTO
  live.PRODUCTS_SILVER
FROM
  STREAM(LIVE.PRODUCTS_BRONZE)
KEYS
  (product_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  seqNum
COLUMNS * EXCEPT
  (operation,seqNum,_rescued_data,ingestion_date,source_path)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------


