# Databricks notebook source
# MAGIC %run "/Workspace/Users/vishal.hinduja@mmc.com/Day 2/includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/formula1_raw

# COMMAND ----------

df=spark.read.csv(f'dbfs:/mnt/cloudthats3/formula1_raw/races.csv',header=True, inferSchema=True)

# COMMAND ----------

df.write.saveAsTable("vishal.results_new_default")

# COMMAND ----------

df.write.option("path","dbfs:/mnt/cloudthats3/output/results_new_nomaint").saveAsTable("vishal.results_new_nomaint")

# COMMAND ----------

hive_metastore.vishal.results_new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select * from `vishal`.`results_new_default` limit 100;

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE vishal.results_new_nomaint

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE vishal.results_new_nomaint

# COMMAND ----------

# MAGIC %sql
# MAGIC create table as select * from delta.'dbfs:/mnt/cloudthats3/output/results_new_nomaint'

# COMMAND ----------


