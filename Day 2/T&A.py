# Databricks notebook source
df=spark.table("vishal.adobe_sample_tbl")

# COMMAND ----------

display(df)

# COMMAND ----------

df.where("topping_id=5002").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.where((col("topping_id")==5003) & (col("batter_type")=="Regular")).display()

# COMMAND ----------

df.where(col("topping_id")=="5003").where(col("batter_type")=="Regular").explain()

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupby("topping_type").count().display()

# COMMAND ----------

df.groupby("topping_type").count().explain()

# COMMAND ----------

df.sort("batter_type").display()

# COMMAND ----------

df.orderBy(col("batter_type").desc()).display()

# COMMAND ----------

df.orderBy(desc("batter_type"),"topping_type").display()

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col("topping_type").like('Chocolate%')).orderBy(col("batter_type").desc(), "topping_type").display()

# COMMAND ----------


