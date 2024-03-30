# Databricks notebook source
input_raw_files="dbfs:/mnt/cloudthats3/formula1_raw"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

output="dbfs:/mnt/cloudthats3/output_formula1/"

# COMMAND ----------


