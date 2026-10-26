# Databricks notebook source
# MAGIC %md
# MAGIC # External Volume vs External Table: When to Use Which?

# COMMAND ----------

# MAGIC %md
# MAGIC **_External Volume_**
# MAGIC
# MAGIC - Used for files such as CSV,JSON, Parquet,images or other file formats 
# MAGIC - Accessed using **/Volume/catalog/schema/volumename**
# MAGIC - Suitable for file ingestion, file excahange and file based processing 
# MAGIC - Does not automatically register fiules as querable sql tables
# MAGIC
# MAGIC
# MAGIC **_External Tables_**
# MAGIC
# MAGIC - Used for structured dataset
# MAGIC - Accessed using catalog.schema.table
# MAGIC - Supports sql queries and dataframe operations
# MAGIC - External Delta tables supports delta operations such as MERGE, UPDATE 

# COMMAND ----------

