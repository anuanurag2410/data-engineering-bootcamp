# Databricks notebook source
# MAGIC %md
# MAGIC # 09_Delta_Merge_Incremental_Loading
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## FULL LOAD VS INCREMENTAL LOAD
# MAGIC
# MAGIC **FULL LOAD**
# MAGIC
# MAGIC - Processes the Complete Data Everytime .
# MAGIC - Often used for initial loads or complete refresh of the data 
# MAGIC - Can Require more compute and processing time 
# MAGIC
# MAGIC
# MAGIC **Incremental LOAD**
# MAGIC
# MAGIC - Processes the New or Changed Records
# MAGIC - Avoids the uneccessary Load of Full Data Refresh
# MAGIC - Requries a reliable method to identify the change 
# MAGIC - Commonly uses **MERGE Comnmand** to target new or changed data

# COMMAND ----------

# MAGIC %md
# MAGIC # UNDERSTANDING DELTA MERGE
# MAGIC
# MAGIC - Source Record Mathces Target --> UPDATE 
# MAGIC - Source Record Does not match the target --> INSERT
# MAGIC - Target record has no matching source --> Leave unchanged , unless we explicitly define a action

# COMMAND ----------

# DBTITLE 1,SYNTAX FOR MERGE INTO
# MAGIC %sql
# MAGIC MERGE INTO target_table AS target 
# MAGIC USING Soure_table as source 
# MAGIC ON target.orderid=sorue.orderid
# MAGIC
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET *
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ### PRACTICAL INCREMNTAL LOAD DEMO USING MERGE INTO

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datax_db.orders_incremental_target
# MAGIC AS SELECT * from datax_db.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from datax_db.orders_incremental_target 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datax_db.orders_incremental_source
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC     (1003, 'C103', 'P102', '2026-09-02', 3, 499.00, 'COMPLETED'),
# MAGIC     (1007, 'C102', 'P103', '2026-09-04', 2, 1499.00, 'COMPLETED'),
# MAGIC     (1011, 'C108', 'P101', '2026-09-06', 1, 899.00, 'PENDING'),
# MAGIC     (1012, 'C109', 'P103', '2026-09-06', 1, 1499.00, 'COMPLETED'),
# MAGIC     (1013, 'C110', 'P102', '2026-09-07', 2, 499.00, 'PENDING')
# MAGIC AS source(
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     product_id,
# MAGIC     order_date,
# MAGIC     quantity,
# MAGIC     unit_price,
# MAGIC     order_status
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from datax_db.orders_incremental_source

# COMMAND ----------

# DBTITLE 1,MERGE SOURCE TO TARGET
# MAGIC %sql
# MAGIC MERGE INTO datax_db.orders_incremental_target as t
# MAGIC USING datax_db.orders_incremental_source as s
# MAGIC
# MAGIC ON s.order_id = t.order_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO datax_db.orders_incremental_target as target
# MAGIC USING datax_db.orders_incremental_source as source
# MAGIC
# MAGIC ON source.order_id = target.order_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         target.customer_id
# MAGIC  = source.customer_id,
# MAGIC         target.product_id = source.product_id,
# MAGIC         target.order_date = source.order_date,
# MAGIC         target.quantity = source.quantity,
# MAGIC         target.unit_price = source.unit_price,
# MAGIC         target.order_status = source.order_status
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         order_id,
# MAGIC         customer_id,
# MAGIC         product_id,
# MAGIC         order_date,
# MAGIC         quantity,
# MAGIC         unit_price,
# MAGIC         order_status
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.order_id,
# MAGIC         source.customer_id,
# MAGIC         source.product_id,
# MAGIC         source.order_date,
# MAGIC         source.quantity,
# MAGIC         source.unit_price,
# MAGIC         source.order_status
# MAGIC     );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY datax_db.orders_incremental_target

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from datax_db.orders_incremental_target