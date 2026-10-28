# Databricks notebook source
# MAGIC %md
# MAGIC # Why Do we need a external Location
# MAGIC
# MAGIC - We could think of external location as storage account keys or secrets direclty 
# MAGIC - Unity catalog procvides a better approach of using the storage ceadentils and external location

# COMMAND ----------

# MAGIC %md
# MAGIC # Storage Credentials VS External locaitons
# MAGIC
# MAGIC
# MAGIC **Storage Credentials**
# MAGIC
# MAGIC - Defines an authentication mechanism 
# MAGIC - In azure, can use an access connector managed identity 
# MAGIC - can be referenced by external locations 
# MAGIC - Does not itself defines which individual files users can access
# MAGIC
# MAGIC
# MAGIC **External Location**
# MAGIC - Defines a cloud storage URL 
# MAGIC - Refrences a storage credential
# MAGIC - Can be used to govern external tables, external voluymes and file access
# MAGIC - Supports unity catalog pivileges.

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

