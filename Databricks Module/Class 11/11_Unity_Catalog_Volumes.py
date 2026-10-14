# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 11_Unity_Catalog_Volumes

# COMMAND ----------

# MAGIC %md
# MAGIC # What is unity Catalog Volume
# MAGIC
# MAGIC - A Unity Catalog Volume is a Goverend Location for storing and accessing files in Databricks 
# MAGIC - It allows you to manage files using Unity Catalog Permissions
# MAGIC
# MAGIC _**A Unity Catalog Volume is a governed storage object that provides file-level access through Unity Catalog. It is used for non-tabular files and file-based Data Engineering workloads.**_

# COMMAND ----------

# MAGIC %md
# MAGIC # TABLE VS VOLUME
# MAGIC
# MAGIC
# MAGIC **_Unity Catalog Table_**
# MAGIC
# MAGIC - Provides govered access to the tables
# MAGIC - Stores your Structured data
# MAGIC - Quiry using sql or Pyspark 
# MAGIC - Supports the Delta Operations like MERGE, UPDATE
# MAGIC
# MAGIC
# MAGIC **_Unity Catalog Volume**
# MAGIC
# MAGIC - Provides govered access to the Files
# MAGIC - Accessed through file paths and APIs.
# MAGIC - Useful for CSV,JSON,Images and other files 
# MAGIC - Example -: /Volumes/dataxdevelopment_7405619193321738/default/delta_datax

# COMMAND ----------

# MAGIC %md
# MAGIC **Question-:** You recieve daily CSV File sfrom an external space application, Would you store them in Unity catalog Table or volume?
# MAGIC
# MAGIC **Answer**
# MAGIC - I will Suggest Volume 
# MAGIC - Then i will land the to volume path 
# MAGIC - Then i will create a script to fetch relevant columns to the bronze table (DELTA)
# MAGIC - Then eventually after all business logics to the GOLDEN Table(DELTA)

# COMMAND ----------

# MAGIC %md
# MAGIC # MANAGED AND EXTERNAL VOLUMES
# MAGIC
# MAGIC **MANAGED Volume**
# MAGIC - Storage location is managed through unity catalog 
# MAGIC - uses the configured managed storage.
# MAGIC - Good for files managed in the Databricks platform 
# MAGIC - Droppign the voliume can also lead to deleting of its underlying managed data, accrodign the the managed-data lifecycle
# MAGIC
# MAGIC **External Volume**
# MAGIC - Points to a Existing cloud storage location 
# MAGIC - Requires the unity catalog external location and appropriate permissions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS 
# MAGIC dataxdevelopment_7405619193321738.bronze.orders_files;

# COMMAND ----------

# Read CSV file from Workspace
order_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Workspace/Users/anuanurag2410@gmail.com/data-engineering-bootcamp/Databricks Module/Class 4/orders.csv")

# Write to Unity Catalog Volume
order_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("/Volumes/dataxdevelopment_7405619193321738/bronze/orders_files")

print("CSV file successfully written to Volume: dataxdevelopment_7405619193321738.bronze.order_files")

# COMMAND ----------

# DBTITLE 1,Read the parquet from teh table
order_files_path="/Volumes/dataxdevelopment_7405619193321738/bronze/orders_files"

order_df=spark.read.option("header", "true").option("inferSchema", "true").csv(order_files_path)
order_df.write.mode("overwrite").saveAsTable("dataxdevelopment_7405619193321738.bronze.order_files")

# COMMAND ----------

print("TOTAL ORDERS -:",order_df.count())

# COMMAND ----------

from pyspark.sql.functions import col

display(order_df.filter(col("order_id").isNull()| col("customer_id").isNull() | col("quantity").isNull() ))

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from dataxdevelopment_7405619193321738.bronze.order_files

# COMMAND ----------

