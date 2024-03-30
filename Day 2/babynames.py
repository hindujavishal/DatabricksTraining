# Databricks notebook source
# MAGIC %run "/Workspace/Users/vishal.hinduja@mmc.com/Day 2/includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/raw/

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/cloudthats3/raw/Baby_Names.csv",header=True, inferSchema=True)

# COMMAND ----------

df.count()

# COMMAND ----------

df.display()

# COMMAND ----------

df_final=df.groupby("Year").count().sort("Year")

# COMMAND ----------

df.write.mode("overwrite").parquet("dbfs:/mnt/cloudthats3/output_formula1/vishal/baby_names")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from parquet.`dbfs:/mnt/cloudthats3/output_formula1/vishal/baby_names`

# COMMAND ----------

df.write.partitionBy("Year").mode("overwrite").parquet("dbfs:/mnt/cloudthats3/output_formula1/vishal/baby_names_by_year")

# COMMAND ----------

df.write.partitionBy("Year","Sex").mode("overwrite").parquet("dbfs:/mnt/cloudthats3/output_formula1/vishal/baby_names_by_year_gender")

# COMMAND ----------


