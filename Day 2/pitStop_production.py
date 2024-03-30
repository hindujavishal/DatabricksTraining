# Databricks notebook source
# MAGIC %run "/Workspace/Users/vishal.hinduja@mmc.com/Day 2/includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/formula1_raw/

# COMMAND ----------

df=spark.read.json(f"{input_raw_files}/pit_stops.json",multiLine=True)

# COMMAND ----------

df_final=df.write.mode("overwrite").parquet(f"{output}vishal/pit_stops")

# COMMAND ----------


