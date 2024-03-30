# Databricks notebook source
# MAGIC %run "/Workspace/Users/vishal.hinduja@mmc.com/Day 2/includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True)

# COMMAND ----------

df.display()

# COMMAND ----------

airline_sch = """
Year int,
Month int,
DayofMonth int, 
DayOfWeek int,
DepTime int, 
CRSDepTime int,
ArrTime int,
CRSArrTime int,
UniqueCarrier string,
FlightNum int,
TailNum string,
ActualElapsedTime int,
CRSElapsedTime int,
AirTime string,
ArrDelay int,
DepDelay int,
Origin string,
Dest string,
Distance int,
TaxiIn string,
TaxiOut string,
Cancelled int,
CancellationCode string,
Diverted int,
CarrierDelay string,
WeatherDelay string,
NASDelay string,
SecurityDelay string,
LateAircraftDelay string
"""

# COMMAND ----------

df=spark.read.schema(airline_sch).csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------


