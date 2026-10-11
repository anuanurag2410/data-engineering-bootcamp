# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
orders_df=spark.read.format("delta").load("/Volumes/dataxdevelopment/default/delta_datax")

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA VS METADATA
# MAGIC
# MAGIC **What is Data?**
# MAGIC
# MAGIC - order_id,customer_id,product_id,order_date,quantity,unit_price,order_status
# MAGIC - 1001,C101,P101,2026-09-01,2,899.00,COMPLETED
# MAGIC - 1002,C102,P103,2026-09-01,1,1499.00,COMPLETED
# MAGIC
# MAGIC
# MAGIC _These kind of information is called as data_
# MAGIC
# MAGIC
# MAGIC **Metadata means**
# MAGIC
# MAGIC TableName --> Orders_delta
# MAGIC Columns --> Order_id
# MAGIC         --> Cusotmer_id
# MAGIC         --> Product_id
# MAGIC         --> Order_date
# MAGIC         --> Quantity
# MAGIC         --> Unit_price
# MAGIC         --> Status
# MAGIC
# MAGIC Format --> Delta 
# MAGIC
# MAGIC location --> Physical Storage Path
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Metastore
# MAGIC
# MAGIC - Metastore is used to maintain metadata about the data objects
# MAGIC - It helps the processing/query engine to understand what object exists and how they are defined

# COMMAND ----------

# MAGIC %md
# MAGIC - Actual Data
# MAGIC -     ↓
# MAGIC - Storage
# MAGIC - And:
# MAGIC - Information about the Data
# MAGIC -         ↓
# MAGIC -      Metadata
# MAGIC -         ↓
# MAGIC -      Metastore
# MAGIC - 

# COMMAND ----------

# MAGIC %md
# MAGIC # HIVE METASTORE
# MAGIC
# MAGIC Tranditionally Databricks Env have used the Hive Metastore to manage the table metadata
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS DataX_DB;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE datax_db

# COMMAND ----------

# MAGIC %sql
# MAGIC Select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

orders_df.write.saveAsTable("orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from orders

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders

# COMMAND ----------

# MAGIC %md
# MAGIC # MANAGED TABLE
# MAGIC
# MAGIC - Autoamtically the location managed by Databricks without any LOCATION keyword
# MAGIC - The platform manages the table storage location for us. 
# MAGIC - The table metadata shows the taba lifecycle as managed

# COMMAND ----------

# MAGIC %md
# MAGIC # External Table
# MAGIC
# MAGIC In External Table we give Explicitly the Location where we want to store the data

# COMMAND ----------

