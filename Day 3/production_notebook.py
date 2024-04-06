# Databricks notebook source
# MAGIC %run "/Workspace/Users/vishal.hinduja@mmc.com/Day 3/prod- includes"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/vishal.hinduja@mmc.com/Day 3/functions"

# COMMAND ----------

dbutils.widgets.text("enviroment"," ")
env=dbutils.widgets.get("enviroment")

# COMMAND ----------

df=spark.read.json(f"{input_path}",multiLine=True)

# COMMAND ----------

df.write.mode("append").option("mergeSchema", "true").saveAsTable("vishal.finance")

# COMMAND ----------

df2=add_column(df)

# COMMAND ----------

df_final=df2.withColumn("environment",lit(env))

# COMMAND ----------

df_final.write.mode("append").option("mergeSchema", "true").saveAsTable("vishal.finance")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vishal.finance

# COMMAND ----------


