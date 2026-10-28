# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ### 12_UC_Tables_Views_Permissions

# COMMAND ----------

# MAGIC %md
# MAGIC - Unity Catalog
# MAGIC - Unity Catalog Volumes 
# MAGIC - Managed Volumes 
# MAGIC - File in PySpark 
# MAGIC - Source Data in DBX

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog Tables 
# MAGIC
# MAGIC - A unity catalog table is a table that is registered and goverened through unity catalog 
# MAGIC - We can query using SQL and Pyspark 
# MAGIC - Unity catalog manages the table metadata and access permissions
# MAGIC - Delta Lake provides transactional capabilities we have already seen it
# MAGIC - MERGE , UPDATE, INSERT DELETE
# MAGIC

# COMMAND ----------

dbutils.widgets.text("catalog", "dataxdevelopment_1", "Unity Catalog")
catalog = dbutils.widgets.get("catalog")
if not catalog:
    raise ValueError("Parameter 'catalog' is required")
spark.sql(f"USE CATALOG `{catalog}`")
print(f"Using catalog: {catalog}")

# COMMAND ----------

orders_path= f"/Volumes/{catalog}/default/datax/orders.csv"
print(orders_path)

# COMMAND ----------

# DBTITLE 1,Defining the Schema Structure
from pyspark.sql.types import (    StructType, StructField,    IntegerType, StringType,    DateType, DecimalType)

orders_schema = StructType([    StructField("order_id", IntegerType()),    
                           StructField("customer_id", StringType()),    
                           StructField("product_id", StringType()),    
                           StructField("order_date", DateType()),    
                           StructField("quantity", IntegerType()),    
                           StructField("unit_price", DecimalType(10, 2)),    
                           StructField("order_status", StringType())])



# COMMAND ----------

orders_df = spark.read   .option("header", "true").schema(orders_schema).csv(orders_path)
display(orders_df)

# COMMAND ----------

orders_df.write.format("delta").mode("overwrite").saveAsTable('bronze.orders_csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from bronze.orders_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe detail bronze.orders_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from bronze.orders_csv order by order_id desc

# COMMAND ----------

# MAGIC %md
# MAGIC # UNITY Catalog Views
# MAGIC
# MAGIC - A view is a saved SQL Query that presentws data from one or more tables 
# MAGIC - A standard view doesnt store a sperate copy of the query result 
# MAGIC - Whenever we execute or query it , then the underlying quey is executed and you see the result
# MAGIC - Views are useful for reuabale busienss logic and controlled access of the data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS GOLD

# COMMAND ----------

# DBTITLE 1,Create View on orders_csv
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW GOLD.vw_orders_completed as
# MAGIC Select 
# MAGIC order_id,
# MAGIC customer_id,
# MAGIC product_id,
# MAGIC order_date, 
# MAGIC quantity, 
# MAGIC unit_price,
# MAGIC quantity*unit_price as Total_Amount
# MAGIC from bronze.orders_csv Where order_status='COMPLETED'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from GOLD.vw_orders_completed

# COMMAND ----------

# MAGIC %md
# MAGIC # USING COMMAND GRAND REVOKE ACCESS ON UNITY CATALOG

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW GRANTS ON CATALOG ${catalog}

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA 
# MAGIC ON SCHEMA ${catalog}.gold
# MAGIC TO `account users`

# COMMAND ----------

# MAGIC %md
# MAGIC