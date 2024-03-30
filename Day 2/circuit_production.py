# Databricks notebook source
# MAGIC %run "/Workspace/Users/vishal.hinduja@mmc.com/Day 2/includes"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the File using path variable, drop url, get ingestion_date column and write into Parquet file

# COMMAND ----------

df=spark.read.csv(f"{input_raw_files}/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df_final = df.drop("url").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df_final.write.mode("overwrite").parquet(f"{output}vishal/circuit")

# COMMAND ----------


