# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run /Workspace/Users/naval@cloudthat.net/Pune/day2/includes

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC {"driverId":1,"driverRef":"hamilton","number":44,"code":"HAM","name":{"forename":"Lewis","surname":"Hamilton"},"dob":"1985-01-07","nationality":"British","url":"http://en.wikipedia.org/wiki/Lewis_Hamilton"}

# COMMAND ----------

user_schema=StructType([StructField("driverId",IntegerType()),
                        StructField("driverRef",StringType()),
                        StructField("number",IntegerType()),
                        StructField("code",StringType()),
                        StructField("name",MapType(StringType(),StringType())),
                        StructField("dob",StringType()),
                        StructField("nationality",StringType()),
                        StructField("url",StringType())
                        ])

# COMMAND ----------

df_auto=spark.read.json(f"{input_raw_files}/drivers.json")

# COMMAND ----------

df_auto.withColumn("forename",col("name.forename"))\
.withColumn("surname",col("name.surname"))\
    .drop("name")\
.display()

# COMMAND ----------

df=spark.read.schema(user_schema).json(f"{input_raw_files}/drivers.json")

# COMMAND ----------

schema_name=StructType([StructField("forename",StringType()),
                        StructField("surname",StringType())
])

# COMMAND ----------

user_schema_2=StructType([StructField("driverId",IntegerType()),
                        StructField("driverRef",StringType()),
                        StructField("number",IntegerType()),
                        StructField("code",StringType()),
                        StructField("name",schema_name),
                        StructField("dob",StringType()),
                        StructField("nationality",StringType()),
                        StructField("url",StringType())
                        ])

# COMMAND ----------

df=spark.read.schema(user_schema_2).json(f"{input_raw_files}/drivers.json")

# COMMAND ----------

ddf=spark.read.json("dbfs:/mnt/cloudthats3/raw_json/array.json")

# COMMAND ----------

display(ddf)

# COMMAND ----------

mobile_schema_name=StructType([StructField("home",IntegerType()),
                        StructField("office",IntegerType())
])

# COMMAND ----------

Array_schema_2=StructType([StructField("id",IntegerType()),
                      StructField("mobile",ArrayType(IntegerType())),
                        StructField("name",StringType())
                        ])

# COMMAND ----------

ddf_struc=spark.read.schema(Array_schema_2).json("dbfs:/mnt/cloudthats3/raw_json/array.json")

# COMMAND ----------

display(ddf_struc)

# COMMAND ----------

ddf_struc.withColumn("exploded_mobile",explode("mobile")).drop("mobile").display()

# COMMAND ----------


