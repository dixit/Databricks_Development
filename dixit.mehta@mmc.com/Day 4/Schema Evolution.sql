-- Databricks notebook source
-- MAGIC %python
-- MAGIC data1=([(1,'a',30),(2,'b',27)])
-- MAGIC schema1="id int, name string, age int"
-- MAGIC df1=spark.createDataFrame(data1,schema1)
-- MAGIC df1.write.saveAsTable("dixit.emp_demo_merge")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data2=([(3,'c',33),(4,'d',37)])
-- MAGIC df2=spark.createDataFrame(data2,schema1)
-- MAGIC df2.write.mode("append").saveAsTable("dixit.emp_demo_merge")

-- COMMAND ----------

select * from dixit.emp_demo_merge;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data3=([(4,'e',39,"IT"),(5,'f',57,"HR")])
-- MAGIC schema3="id int, name string, age int, dept string"
-- MAGIC df3=spark.createDataFrame(data3,schema3)
-- MAGIC df3.write.mode("append").saveAsTable("dixit.emp_demo_merge")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data3=([(4,'e',39,"IT"),(5,'f',57,"HR")])
-- MAGIC schema3="id int, name string, age int, dept string"
-- MAGIC df3=spark.createDataFrame(data3,schema3)
-- MAGIC df3.write.mode("append").option("mergeschema",True).saveAsTable("dixit.emp_demo_merge")

-- COMMAND ----------

select * from dixit.emp_demo_merge;

-- COMMAND ----------


