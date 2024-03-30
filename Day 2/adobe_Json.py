# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/raw_json/

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/cloudthats3/raw_json/adobe_sample_json.json",multiLine=True)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")\
.display()

# COMMAND ----------

df_final =df.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")\
.withColumn("batters",explode("batters.batter"))\
.withColumn("batter_id",col("batters.id"))\
.withColumn("batter_type",col("batters.type"))\
.drop("batters")

# COMMAND ----------

display(df_final)

# COMMAND ----------

df_final.write.saveAsTable("vishal.adobe_sample_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vishal.adobe_sample_tbl where topping_id=5001

# COMMAND ----------

df=spark.read.table('vishal.adobe_sample_tbl')

# COMMAND ----------


