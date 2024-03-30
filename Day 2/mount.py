# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/formula1_raw/

# COMMAND ----------

input_raw_files="dbfs:/mnt/cloudthats3/formula1_raw"

# COMMAND ----------

df=spark.read.csv(f"{input_raw_files}/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------


