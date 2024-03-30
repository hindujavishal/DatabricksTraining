# Databricks notebook source
# MAGIC %run "/Workspace/Users/vishal.hinduja@mmc.com/Day 2/includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/formula1_raw/lap_times/

# COMMAND ----------

df=spark.read.csv(f"{input_raw_files}/lap_times/*")

##,header=true,inferSchema=True)

# COMMAND ----------

laptime_schema = "raceId int, driverId int, lap int, position int, time string, millisecond int"

# COMMAND ----------

df=spark.read.schema(laptime_schema).csv(f"{input_raw_files}/lap_times/*")

# COMMAND ----------

df.display()

# COMMAND ----------

df_final=df.write.mode("overwrite").parquet(f"{output}vishal/lap_times")

# COMMAND ----------


