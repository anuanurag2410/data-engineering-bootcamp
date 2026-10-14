# Databricks notebook source
# MAGIC %md
# MAGIC # 10_Unity_Catalog_Fundamentals
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # What is Unity Catalog? 
# MAGIC
# MAGIC - Unity Catalog is Databricks Centralized System from managing data access, metadata and governance.
# MAGIC - It Helps us organize the data nd control who uses it 
# MAGIC - It provides capabilites data lineage and auditing.
# MAGIC - Unity Catalog will help you to manage these kind of different permissions 
# MAGIC - it also helps you to organize the data assests accross the teams and environments
# MAGIC
# MAGIC
# MAGIC **Lets Take example of Orders Table**
# MAGIC
# MAGIC - Data Engineer will be responsible to crete and udpate table 
# MAGIC - Data Analyts will be responsible to Read the table and do thier analytics
# MAGIC
# MAGIC
# MAGIC
# MAGIC _**Unity Catalog provides centralized governance for data assets in Databricks, including access control, metadata management, auditing and lineage**_
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # HIVE METASTORE V/S UNITY CATALOG
# MAGIC
# MAGIC **_Hive Metastore_**
# MAGIC - Traditional Naming: _database.table_
# MAGIC - Example : _datax_db.orders_
# MAGIC - Legacy Workspace-Level Metastore
# MAGIC - DBFS(database File System) it is non Goverened
# MAGIC
# MAGIC
# MAGIC **_Unity Catalog_**
# MAGIC - Naming: _catalog.database.table_
# MAGIC - Example : _datax_development.datax_db.orders_
# MAGIC - Three Level Namespace
# MAGIC - Centralized Governace Across the Attached Workspace 
# MAGIC - Fine-Grained Access Controls 
# MAGIC - Data Lineage and Data Auditing Capabilites 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Understand the Unity Catalog Hierarchy
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE SCHEMA IF NOT EXISTS dataxdevelopment_7405619193321738.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS dataxdevelopment_7405619193321738.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS dataxdevelopment_7405619193321738.gold;

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS dataxdevelopment_7405619193321738.bronze.orders_uc_demo_bronze
# MAGIC AS 
# MAGIC SELECT * FROM dataxdevelopment_7405619193321738.datax_db.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dataxdevelopment_7405619193321738.bronze.orders_uc_demo_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC # DEVELOPMENT Environment
# MAGIC
# MAGIC - dataxdevelopment_7405619193321738.bronze.orders
# MAGIC - dataxdevelopment_7405619193321738.silver.orders
# MAGIC - dataxdevelopment_7405619193321738.gold.daily_sales
# MAGIC
# MAGIC
# MAGIC # PRODUCTION Environment
# MAGIC
# MAGIC - dataxproduction_7405619193321738.bronze.orders
# MAGIC - dataxproduction_7405619193321738.silver.orders
# MAGIC - dataxproduction_7405619193321738.gold.daily_sales
# MAGIC

# COMMAND ----------

