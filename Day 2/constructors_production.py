# Databricks notebook source
# MAGIC %run "/Workspace/Users/vishal.hinduja@mmc.com/Day 2/includes"

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create or replace table vishal.constructorsfinal AS
# MAGIC --Select *,current_timestamp() AS ingestion_date from json.`dbfs:/mnt/cloudthats3/formula1_raw/constructors.json`

# COMMAND ----------

input_raw_files

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from vishal.constructorsfinal

# COMMAND ----------

df=spark.read.json(f"{input_raw_files}/constructors.json")

# COMMAND ----------

df_final=df.write.mode("overwrite").parquet(f"{output}vishal/constructors")

# COMMAND ----------

df=spark.read.parquet(f"{output}vishal/constructors")

# COMMAND ----------

output

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/mnt/cloudthats3/output_formula1/vishal/constructors`

# COMMAND ----------


