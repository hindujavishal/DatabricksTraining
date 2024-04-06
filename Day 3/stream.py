# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/stream/input/csv_raw/emp/

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`dbfs:/mnt/cloudthats3/stream/input/csv_raw/emp/Jan.CSV`

# COMMAND ----------

user_schema="id int, Name string, Gender string, Salary int, Country string, Date string"

# COMMAND ----------

(spark
.readStream
.schema(user_schema)
.csv('dbfs:/mnt/cloudthats3/stream/input/csv_raw/emp/',header=True)
.writeStream
.option("checkpointLocation","dbfs:/mnt/cloudthats3/stream/stream_pune/vishal/stream_first/checkpiont")
.trigger(processingTime="1 minute")
.table("vishal.stream_first")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vishal.stream_first

# COMMAND ----------


