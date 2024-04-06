-- Databricks notebook source
-- MAGIC %python
-- MAGIC data=([(1,'a',30),(2,"b",32)])
-- MAGIC schema="id int, name string, age int"
-- MAGIC df=spark.createDataFrame(data,schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.saveAsTable("ny_delta.emp")

-- COMMAND ----------

select * from ny_delta.emp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data=([(3,'a',30,"Sales"),(4,"b",32,"IT")])
-- MAGIC schema="id int, name string, age int,dept string"
-- MAGIC df=spark.createDataFrame(data,schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("append").saveAsTable("ny_delta.emp")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("append").option("mergeSchema", "true").saveAsTable("ny_delta.emp")

-- COMMAND ----------

select * from ny_delta.emp

-- COMMAND ----------

describe history ny_delta.emp

-- COMMAND ----------


