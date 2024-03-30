# Databricks notebook source
# MAGIC %fs  ls dbfs:/FileStore/tables

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/formula1_raw/

# COMMAND ----------

df=spark.read.json('dbfs:/FileStore/tables/formula1_raw/constructors.json')

# COMMAND ----------

df.drop('url').withColumn("ingestion_date",current_timestamp()).display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.drop('url').withColumn("ingestion_date",current_timestamp()).display()

# COMMAND ----------

df1= df.drop('url').withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df1.write.saveAsTable('vishal.constructors')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from vishal.constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table vishal.constructorsfinal AS
# MAGIC Select *,current_timestamp() AS ingestion_date from json.({input_raw_files}/vishal\constructors.json`)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from vishal.constructors2

# COMMAND ----------


