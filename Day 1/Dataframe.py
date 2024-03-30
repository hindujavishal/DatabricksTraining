# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ######show the contents of folder

# COMMAND ----------

# MAGIC %fs  ls dbfs:/FileStore/tables/formula1_raw/

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step1 - Create DF from File

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Transform

# COMMAND ----------

df.select('*').display()

# COMMAND ----------

df.select('circuitId').display()

# COMMAND ----------

from python.SQL import col

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df.select(col('circuitId').alias('circuit_id')).display()

# COMMAND ----------

df.select("circuitId",col("circuitRef"),df.name,df["location"]).display()

# COMMAND ----------

from pyspark.sql.functions import concat

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(concat('location','country')).display()

# COMMAND ----------

df.select(concat('location',lit(' '),'country')).display()

# COMMAND ----------

df.select(concat('location',lit(', '),'country').alias('location, country')).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("location","city").display()

# COMMAND ----------

df.columns

# COMMAND ----------

new_column_name = ('circuitId',
 'circuitRef',
 'name',
 'city',
 'country',
 'latitude',
 'longitude',
 'alt',
 'url')

# COMMAND ----------

df1 = df.toDF(*new_column_name)

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.drop('circuitid')

# COMMAND ----------

df1.drop('circuitid').display()

# COMMAND ----------

df2 = df1.drop('circuitid')

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.withColumn("current_time",current_date()).display()

# COMMAND ----------

df1.withColumn("upper_name",upper("name")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Load

# COMMAND ----------

df.write.parquet('dbfs:/FileStore/tables/output/vishal/circuit')

# COMMAND ----------

df=spark.read.parquet('dbfs:/FileStore/tables/output/vishal/circuit')

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists vishal

# COMMAND ----------

df1.write.parquet('dbfs:/FileStore/tables/output/vishal/circuit')

# COMMAND ----------

df1.write.parquet('dbfs:/FileStore/tables/output/vishal/circuit',mode='overwrite')

# COMMAND ----------

df1=spark.read.parquet('dbfs:/FileStore/tables/output/vishal/circuit')

# COMMAND ----------

df1.write.saveAsTable('vishal.circuit')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vishal.circuit

# COMMAND ----------

select city, count(circuitid) from vishal.circuit group by city

# COMMAND ----------

# MAGIC %sql
# MAGIC select city, count(circuitid) from vishal.circuit group by city

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, count(circuitid) from vishal.circuit group by country

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, count(circuitid) from vishal.circuit group by country order by 2 DESC

# COMMAND ----------


