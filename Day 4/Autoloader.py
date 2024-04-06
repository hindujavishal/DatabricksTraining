# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/auto_stream/input/csv

# COMMAND ----------

input_path="dbfs:/mnt/cloudthats3/auto_stream/input/csv"
output_path="dbfs:/mnt/cloudthats3/auto_stream/output"

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation",f"{output_path}/vishal/autoloader_csv/schema")
 .load(f"{input_path}")
 .writeStream
 .option("checkpointLocation",f"{output_path}/vishal/autoloader_csv/checkpoint")
 .option("path",f"{output_path}/vishal/autoloader_csv/table")
 .trigger(once=True)
 .table("vh_delta.autoloader_csv")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vh_delta.autoloader_csv

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation",f"{output_path}/vishal/autoloader_csv/schema")
 .option("cloudFiles.inferColumnTypes",True)
 .load(f"{input_path}")
 .writeStream
 .option("checkpointLocation",f"{output_path}/vishal/autoloader_csv/checkpoint")
 .option("path",f"{output_path}/vishal/autoloader_csv/table")
 .trigger(once=True)
 .table("vh_delta.autoloader_csv")
)

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation",f"{output_path}/vishal/autoloader_csv/schema")
 .option("cloudFiles.schemaEvolutionMode","rescue")
 .option("cloudFiles.inferColumnTypes",True)
 .load(f"{input_path}")
 .writeStream
 .option("checkpointLocation",f"{output_path}/vishal/autoloader_csv/checkpoint")
 .option("path",f"{output_path}/vishal/autoloader_csv/table")
 .option("mergeSchema",True)
 .table("vh_delta.autoloader_csv")
)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/auto_stream/input/

# COMMAND ----------

json_input_path = "dbfs:/mnt/cloudthats3/auto_stream/input/json/"

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .option("cloudFiles.schemaLocation",f"{output_path}/vishal/autoloader_json2/schema")
 .option("cloudFiles.schemaEvolutionMode","rescue")
.option("cloudFiles.inferColumnTypes",True)
 .load(f"{json_input_path}")
 .writeStream
 .option("checkpointLocation",f"{output_path}/vishal/autoloader_json2/checkpoint")
 .option("path",f"{output_path}/vishal/autoloader_json2/table")
 .option("mergeSchema",True)
 .trigger(once=True)
 .table("vh_delta.autoloader_json2")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vh_delta.autoloader_json2

# COMMAND ----------


