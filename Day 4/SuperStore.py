# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/superstore/

# COMMAND ----------

input_path="dbfs:/mnt/cloudthats3/superstore/"
output_path="dbfs:/mnt/cloudthats3/auto_stream/output"

# COMMAND ----------

df=(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.inferColumnTypes","True")
 .option("cloudFiles.schemaLocation",f"{output_path}/vishal/superstore/schema")
 .option("cloudFiles.schemaEvolutionMode","rescue")
 .load(f"{input_path}"))

# COMMAND ----------

df = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "csv")
                  .option("cloudFiles.schemaLocation", f"{output_path}/vishal/superstore/schema1")
                 .option("cloudFiles.inferColumnTypes","True")
                  .option("header", "True")
                  .load(f"{input_path}"))

# COMMAND ----------

df.createOrReplaceTempView("superstore")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view superstore_bronze
# MAGIC select
# MAGIC Row ID as row_id
# MAGIC Order ID as order_id
# MAGIC Order Date as order_dt
# MAGIC Ship Date as ship_dt
# MAGIC Ship Mode:string
# MAGIC Customer ID:string
# MAGIC Customer Name:string
# MAGIC Segment:string
# MAGIC Country/Region:string
# MAGIC City:string
# MAGIC State:string
# MAGIC Postal Code:integer
# MAGIC Region:string
# MAGIC Product ID:string
# MAGIC Category:string
# MAGIC Sub-Category:string
# MAGIC Product Name:string
# MAGIC Sales:string
# MAGIC Quantity:string
# MAGIC Discount:double
# MAGIC Profit:double
# MAGIC _rescued_data:string
# MAGIC
# MAGIC  Row ID as row_id, Order ID as order_id, Order Date as order_date,Ship Date as ship_date, Ship Mode as ship_mode ,Customer ID as customer_id, Customer Name as customer_name ,Segment ,Country/Region as country_region, City, State,Postal Code postal_code, Region, Product ID product_id, Category, Sub-Category, Product Name product_name,Sales ,Quantity,Discount,Profit,_rescued_data current_timestamp() as ingestion_date, input_file_name() as source_path from superstore

# COMMAND ----------


