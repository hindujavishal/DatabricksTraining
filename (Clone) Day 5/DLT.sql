-- Databricks notebook source
--%fs ls dbfs:/mnt/cloudthats3/dlt/

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_customers
COMMENT "The customers bronze table"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
AS SELECT *, current_timestamp() AS ingestion_date, input_file_name() as source_path FROM cloud_files("dbfs:/mnt/cloudthats3/dlt/customers/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_order_date
COMMENT "The Order Date bronze table"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
AS SELECT * , current_timestamp() AS ingestion_date, input_file_name() as source_path FROM cloud_files("dbfs:/mnt/cloudthats3/dlt/order_date/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_product
COMMENT "The Product bronze table"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
AS SELECT * , current_timestamp() AS ingestion_date, input_file_name() as source_path  FROM cloud_files("dbfs:/mnt/cloudthats3/dlt/products/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE OR REPLACE STREAMING LIVE TABLE bronze_sales
COMMENT "The Sales bronze table"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
AS SELECT *, current_timestamp() AS ingestion_date, input_file_name() as source_path FROM cloud_files("dbfs:/mnt/cloudthats3/dlt/sales/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_customers_cleaned
(CONSTRAINT valid_customer_id EXPECT (customer_id is NOT NULL) on VIOLATION DROP ROW)
COMMENT "The Customer  Cleaned Silver  table"
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS SELECT DISTINCT *  FROM STREAM(LIVE.bronze_customers);

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_order_dt_cleaned
(CONSTRAINT valid_order_id EXPECT (order_date is NOT NULL) on VIOLATION DROP ROW)
--, CONSTRAINT order_id_duplicates EXPECT ( order_id)  on VIOLATION DROP ROW)
COMMENT "The Order Dt Cleaned Silver  table"
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS SELECT DISTINCT * FROM STREAM(LIVE.bronze_order_date);

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_products_cleaned
(CONSTRAINT valid_product_id EXPECT (product_id is NOT NULL) on VIOLATION DROP ROW)
COMMENT "The Products  Silver  table"
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS SELECT DISTINCT product_id, product_name, product_category, product_price, operation, seqNum, ingestion_date, source_path  FROM STREAM(LIVE.bronze_product);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_products;

APPLY CHANGES INTO LIVE.silver_products
  FROM STREAM(LIVE.silver_products_cleaned)
  KEYS (product_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY seqNum
  COLUMNS * EXCEPT (operation, source_path, ingestion_date)


-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_sales_cleaned
(CONSTRAINT valid_order_id EXPECT (order_id is NOT NULL) on VIOLATION DROP ROW)
--, CONSTRAINT order_id_duplicates EXPECT ( order_id)  on VIOLATION DROP ROW)
COMMENT "The Sales Silver  table"
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS SELECT DISTINCT order_id,customer_id,transaction_id,product_id, quantity, total_amount, order_date FROM STREAM(LIVE.bronze_sales);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_customers;

APPLY CHANGES INTO LIVE.silver_customers
  FROM STREAM(LIVE.silver_customers_cleaned)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, source_path, ingestion_date, _rescued_data)
  STORED AS
  SCD Type 2


-- COMMAND ----------

CREATE LIVE TABLE gold_product_orders 
COMMENT "Summary of Product categories by Sales."
TBLPROPERTIES ("myCompanyPipeline.quality" = "gold")
AS
SELECT p.product_category, sum(s.total_amount) AS TotalSales, sum(s.quantity) AS TotalQty 
FROM LIVE.silver_products p
LEFT JOIN LIVE.silver_sales s
ON p.product_id = s.product_id
GROUP BY p.product_category

-- COMMAND ----------

-- MAGIC %python
-- MAGIC schema="vishal"

-- COMMAND ----------

SELECT * FROM dbutils.widgets.get("state")
silver_customers

-- COMMAND ----------


