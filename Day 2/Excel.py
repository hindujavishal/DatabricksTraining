# Databricks notebook source
# MAGIC %run "/Workspace/Users/vishal.hinduja@mmc.com/Day 2/includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/raw/

# COMMAND ----------



# COMMAND ----------

df=spark.read.format("com.crealytics.spark.excel").load("dbfs:/mnt/cloudthats3/raw/emp.xlsx",header=True, inFerSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------


