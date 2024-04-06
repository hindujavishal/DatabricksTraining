# Databricks notebook source
from pyspark.sql.functions import *
def add_column(input_df):
    output=input_df.withColumn("ingestion_date",current_timestamp())
    return output

# COMMAND ----------


