# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # External Managed Tables

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed Table and External**
# MAGIC
# MAGIC A table has Metadata
# MAGIC The Actual Records are persisted in the storage. 
# MAGIC Depending on how we create the table its life cycle is managed differenty

# COMMAND ----------

# MAGIC %sql
# MAGIC USE datax_db;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC # WHAT IS MANAGED TABLE?
# MAGIC
# MAGIC - Databricks/metastore manages the table metadata.
# MAGIC - We don't manually specify an external data location when creating it.
# MAGIC - The system determines the managed storage location.
# MAGIC - The table's data lifecycle is associated with the table.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe Extended datax_db.orders

# COMMAND ----------

managed_detail=spark.sql("""Describe Extended datax_db.orders""")
display(managed_detail)

# COMMAND ----------

managed_location = (    spark.sql("DESCRIBE DETAIL datax_db.orders")    .select("location")    .first()[0])
print(managed_location)

# COMMAND ----------

# MAGIC %md
# MAGIC # INTRODUCING EXTERNAL TABLES
# MAGIC
# MAGIC _The External Table says_
# MAGIC
# MAGIC - Metadata registered in Metastore 
# MAGIC - Data is stored explicitly in another specified location 
# MAGIC - Storage lifecycle is controlled independenly from the table metadata

# COMMAND ----------

orders_df=spark.read.format("delta").load("/Volumes/dataxdevelopment_7405619193321738/default/delta_datax")

# COMMAND ----------

orders_df=spark.read.format("delta").table("dataxdevelopment_7405619193321738.default.orders_delta")
display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC https://dataxazure.blob.core.windows.net/dev-datax/landing/processed_orders/2026/04/07/SampleData%20-%20orders_bad_20260406%20(1).csv

# COMMAND ----------

# DBTITLE 1,External Location Setup
# MAGIC %md
# MAGIC ## Setting Up External Location for Azure Storage
# MAGIC
# MAGIC To write data to an external Azure storage path, Unity Catalog needs:
# MAGIC 1. **Storage Credential** — authenticates to the Azure storage account (uses an Access Connector managed identity)
# MAGIC 2. **External Location** — maps a cloud storage path to that credential
# MAGIC
# MAGIC > **Prerequisites:** Your Azure admin must grant the **Storage Blob Data Contributor** role on the `dataxazure` storage account to the Databricks Access Connector's managed identity.

# COMMAND ----------

# DBTITLE 1,Step 1: Create Storage Credential
# MAGIC %sql
# MAGIC -- Step 1: Create a storage credential using your Azure Access Connector
# MAGIC -- Replace <access-connector-resource-id> with your actual Access Connector resource ID
# MAGIC -- Format: /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Databricks/accessConnectors/<connector-name>
# MAGIC
# MAGIC SHOW STORAGE CREDENTIALS;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE STORAGE CREDENTIAL dataxazure_credential;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     external_location_name,
# MAGIC     url,
# MAGIC     storage_credential_name,
# MAGIC     storage_credential_id,
# MAGIC     read_only
# MAGIC FROM system.information_schema.external_locations
# MAGIC WHERE external_location_name = 'dataxazure_landing';

# COMMAND ----------

# External location V2
orders_path = (
    "abfss://dev-datax@dataxazure.dfs.core.windows.net/landing/processed_orders/Sample_New/"
)


# Write orders DataFrame as Delta
orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(orders_path)

print("Orders data written successfully!")

# COMMAND ----------

display(orders_df.limit(10))

# COMMAND ----------

path = "abfss://dev-datax@dataxazure.dfs.core.windows.net/landing/"

display(dbutils.fs.ls(path))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE datax_db.orders_external 
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://dev-datax@dataxazure.dfs.core.windows.net/landing/processed_orders/Sample/'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED datax_db.orders_external 

# COMMAND ----------

# MAGIC %md
# MAGIC # When Does managed table makes sense?
# MAGIC
# MAGIC - Databricks owns/manages the table lifecycle 
# MAGIC - We don't need independent management of the physical data location.
# MAGIC - We want simpler table management.
# MAGIC
# MAGIC
# MAGIC # When Does External table makes sense?
# MAGIC
# MAGIC - Data already exists at a controlled storage location
# MAGIC - Storage lifecyle must remain independent
# MAGIC - organization wants explicit control over where data resides.
# MAGIC

# COMMAND ----------

