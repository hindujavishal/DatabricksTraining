# Databricks notebook source
# MAGIC %run "/Workspace/Users/vishal.hinduja@mmc.com/Day 3/prod- includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/institute_data/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/institute_data/feedback/

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`dbfs:/mnt/cloudthats3/institute_data/feedback/Feedback.csv`

# COMMAND ----------

df=spark.read.csv('dbfs:/mnt/cloudthats3/institute_data/feedback/Feedback.csv', header=True)

# COMMAND ----------

df.display()

# COMMAND ----------

feedback_schema = "Registration_id string, Timestamp string, Email_Address string, Email_Id string, Full_Name string, WhatsApp_Contact string, Rating string, suggestions string, interested string, time string"

# COMMAND ----------

(spark
.readStream
.schema(feedback_schema)
.csv('dbfs:/mnt/cloudthats3/institute_data/feedback/',header=True)
.writeStream
.option("checkpointLocation","dbfs:/mnt/cloudthats3/stream/stream_pune/vishal/enrollment/feedback/checkpiont")
.trigger(availableNow=True)
.table("vishal.vh_feedback_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vishal.vh_feedback_bronze

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/institute_data/registration/

# COMMAND ----------

df2=spark.read.csv('dbfs:/mnt/cloudthats3/institute_data/registration/Registration.csv', header=True, inferSchema=True, multiLine=True)

# COMMAND ----------

registration_schema = "Registration_id string, Timestamp string, Email_Address string, Email_Id string, Full_Name string, WhatsApp string, State_Country string, Degree string, Occupation string, Job_Title string, Organization string, Experience string, Skills string, Course_id string"

# COMMAND ----------

(spark
.readStream
.schema(registration_schema)
.csv('dbfs:/mnt/cloudthats3/institute_data/registration/',header=True, inferSchema=True, multiLine=True)
.writeStream
.option("checkpointLocation","dbfs:/mnt/cloudthats3/stream/stream_pune/vishal/enrollment/registration/checkpoint_new")
.trigger(availableNow=True)
.table("vishal.vh_registration_bronze")
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vishal.vh_registration_bronze

# COMMAND ----------

df_reg_sil=spark.read.format(delta).'vishal.vh_registration_bronze'


# COMMAND ----------


