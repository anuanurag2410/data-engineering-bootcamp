# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ### 12_UC_Tables_Views_Permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unity Catalog Tables
# MAGIC
# MAGIC - A unity Catalog Tables is a table registered and Goverened through unity catalog.
# MAGIC - We can query this on Pyspark or SQL
# MAGIC - Unity Catalog Manages your metadata and access permissions
# MAGIC - Delta Lake Providoes the transactional capabilities we have already covered those things
# MAGIC
# MAGIC _There are two types of tables_
# MAGIC
# MAGIC 1- Managed Table
# MAGIC
# MAGIC 2- External Table

# COMMAND ----------

# Unity Catalog name is passed as a parameter from the calling notebook
# e.g. dbutils.notebook.run("11_Unity_Catalog_Volumes", 0, {"catalog": "<catalog_name>"})
# SQL cells reference it as :catalog via IDENTIFIER()
dbutils.widgets.text("catalog", "dataxdev", "Unity Catalog")
catalog = dbutils.widgets.get("catalog")
if not catalog:
    raise ValueError("Parameter 'catalog' is required")
spark.sql(f"USE CATALOG `{catalog}`")
print(f"Using catalog: {catalog}")

# COMMAND ----------

# DBTITLE 1,Creating Unity Catalog Delta Table
orders_path=f"/Volumes/{catalog}/bronze/orders_files"

# COMMAND ----------

from pyspark.sql.types import ( StructType, StructField, IntegerType, StringType,  DateType, DecimalType)

orders_schema=StructType([    StructField("order_id", IntegerType()),    
                          StructField("customer_id", StringType()),   
                           StructField("product_id", StringType()),    
                           StructField("order_date", DateType()),    
                           StructField("quantity", IntegerType()),    
                           StructField("unit_price", DecimalType(10, 2)),    
                           StructField("order_status", StringType())])

orders_df=spark.read.option("header","true").schema(orders_schema).csv(orders_path)
display(orders_df)

# COMMAND ----------

orders_df.write.format('delta').mode("overwrite").saveAsTable("bronze.orders_csv")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DETAIL bronze.orders_csv

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT IS VIEW**
# MAGIC
# MAGIC - View is a saved SQL Query that presents data from one or more tables.
# MAGIC - A Standard View doent sotre a sepreate copy of your query result.
# MAGIC - WHenever we query it, Databricks executes the underlying query.
# MAGIC - Views are useful for reusable business logic and controlled access of data.

# COMMAND ----------

# DBTITLE 1,Create a View Practically
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW bronze.vw_orders_csv AS
# MAGIC SELECT 
# MAGIC
# MAGIC order_id,
# MAGIC customer_id,
# MAGIC product_id,
# MAGIC order_date,
# MAGIC quantity,
# MAGIC unit_price,
# MAGIC order_status,
# MAGIC quantity*unit_price as ORDER_AMOUNT
# MAGIC
# MAGIC FROM bronze.orders_csv
# MAGIC WHERE order_status='COMPLETED'
# MAGIC