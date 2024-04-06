-- Databricks notebook source
https://docs.databricks.com/en/delta/delta-change-data-feed.html#enable-change-data-feed

-- COMMAND ----------

-- MAGIC %python
-- MAGIC countries = [("USA", 10000, 20000), ("India", 1000, 1500), ("UK", 7000, 10000), ("Canada", 500, 700) ]
-- MAGIC columns = ["Country","NumVaccinated","AvailableDoses"]
-- MAGIC spark.createDataFrame(data=countries, schema = columns).write.mode("overwrite").saveAsTable("vh_delta.silverTable")

-- COMMAND ----------

select * from Vh_delta.silverTable

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC (spark.read.table("vh_delta.silverTable")
-- MAGIC .withColumn("VaccinationRate",col("NumVaccinated")/col("AvailableDoses"))
-- MAGIC .drop("NumVaccinated","AvailableDoses")
-- MAGIC .write.mode("overwrite").saveAsTable("vh_delta.goldTable"))

-- COMMAND ----------

select * from vh_delta.goldTable

-- COMMAND ----------

ALTER TABLE vh_delta.silvertable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

describe extended vh_delta.silvertable

-- COMMAND ----------

-- MAGIC %python
-- MAGIC new_countries = [("Australia", 100, 3000)]
-- MAGIC columns = ["Country","NumVaccinated","AvailableDoses"]
-- MAGIC spark.createDataFrame(data=new_countries, schema = columns).write.format("delta").mode("append").saveAsTable("vh_delta.silvertable")

-- COMMAND ----------

-- update a record
UPDATE vh_delta.silvertable SET NumVaccinated = '11000' WHERE Country = 'USA'

-- COMMAND ----------

-- delete a record
DELETE from vh_delta.silvertable WHERE Country = 'UK'

-- COMMAND ----------

select * from vh_delta.silvertable

-- COMMAND ----------

describe history vh_delta.silvertable

-- COMMAND ----------

select * from table_changes('vh_delta.silvertable', 2,4 )

-- COMMAND ----------

SELECT *, rank() over (partition by Country order by _commit_version desc) as rank
          FROM table_changes('vh_delta.silvertable', 2,4)
          where _change_type!='update_preimage'

-- COMMAND ----------

-- Collect only the latest version for each country
CREATE OR REPLACE TEMPORARY VIEW vh_silverTable_latest_version as
SELECT * 
    FROM 
         (SELECT *, rank() over (partition by Country order by _commit_version desc) as rank
          FROM table_changes('vh_delta.silvertable', 2,4)
          WHERE _change_type !='update_preimage')
    WHERE rank=1

-- COMMAND ----------

-- Merge the changes to gold
MERGE INTO vh_delta.goldTable t USING vh_silverTable_latest_version s ON s.Country = t.Country
        WHEN MATCHED AND s._change_type='update_postimage' THEN UPDATE SET VaccinationRate = s.NumVaccinated/s.AvailableDoses
         WHEN MATCHED AND s._change_type='delete' THEN DELETE 
        WHEN NOT MATCHED THEN INSERT (Country, VaccinationRate) VALUES (s.Country, s.NumVaccinated/s.AvailableDoses)

-- COMMAND ----------

select * from vh_delta.goldTable

-- COMMAND ----------

select * from vh_delta.silverTable

-- COMMAND ----------


