# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/deltalake_pune/

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA vh_delta location 'dbfs:/mnt/cloudthats3/deltalake_pune/vishal'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS vh_delta.people10m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC )

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/deltalake_pune/vishal/people10m/_delta_log/

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY vh_delta.people10m

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/cloudthats3/deltalake_pune/vishal/people10m/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO vh_delta.people10m VALUES(2,'a','l','b',"Male", '2024-04-04T00:00:00','77799', 1000),(3,'b','l','c',"Male", '2024-04-04T00:00:00','77799', 1000),
# MAGIC (4,'c','l','d',"Male", '2024-04-04T00:00:00','77799', 1000), (5,'x','l','y',"Male", '2024-04-04T00:00:00','77799', 1000)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/deltalake_pune/vishal/people10m/_delta_log

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/deltalake_pune/vishal/people10m/

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/cloudthats3/deltalake_pune/vishal/people10m/_delta_log/00000000000000000002.json

# COMMAND ----------

# MAGIC %sql
# MAGIC SeLECT * FROM vh_delta.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY vh_delta.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vh_delta.people10m version AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM vh_delta.people10m
# MAGIC where id=4

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/deltalake_pune/vishal/people10m/

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM vh_delta.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY vh_delta.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE vh_delta.people10m version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY vh_delta.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vh_delta.people10m

# COMMAND ----------


