# Databricks notebook source
# MAGIC %md
# MAGIC # 07_Delta_Lake_Fundamentals
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet VS Delta Lake
# MAGIC
# MAGIC - Delta is not replacing Parquet
# MAGIC - Delta Lake Tables Store their actual data in **parquet Files**
# MAGIC - On top those files, Delta Maintains a Transaction Log
# MAGIC - This transaction logs helps Delta track committed changed and maintained a consistent view of the table 

# COMMAND ----------

# MAGIC %md
# MAGIC **Plain Parquet**
# MAGIC
# MAGIC - Columnar Storage Format 
# MAGIC - Efficient Compression
# MAGIC - Schema Information is Stored in Files
# MAGIC - No-Build ACID Transaction 
# MAGIC - No-Build in UPDATE ,DELETE or MERGE
# MAGIC - No-Build in Time Travel or Table History
# MAGIC
# MAGIC **Delta Lake**
# MAGIC
# MAGIC - Uses Parquet File Format to store the Actual Data
# MAGIC - Adds a Delta Transactional Log Folder 
# MAGIC - Supports ACID Transactions Logs
# MAGIC - Supports UPDATE ,DELETE or MERGE
# MAGIC - Maintains the Table History
# MAGIC - And Also You can do a Time Travel on the Delta Lake Table
# MAGIC - Provides the Schema Enforcement

# COMMAND ----------

print("DataX Compute is up.....")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM datax_db.orders
# MAGIC ORDER BY order_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL datax_db.orders

# COMMAND ----------

table_path = (spark.sql("DESCRIBE DETAIL datax_db.orders")    .select("location")    .first()[0])
print(table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC **abfss://unity-catalog-storage@dbstorageci3jq6rqioaye.dfs.core.windows.net/7405608929107032/__unitystorage/catalogs/a9e7e8e3-2077-4051-a92c-0001af1f39a7/tables/ace46c95-158b-4fa8-ae44-645fc22ff8b3**

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from datax_db.orders Where order_id='1002'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from datax_db.orders

# COMMAND ----------

df = spark.table("datax_db.orders")
df.write.format("delta").mode("overwrite").save("/Volumes/dataxdevelopment_7405619193321738/default/delta_datax")

# COMMAND ----------

# MAGIC %sql 
# MAGIC RESTORE TABLE datax_db.orders VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from datax_db.orders

# COMMAND ----------

# MAGIC %md
# MAGIC # ACID Transactions
# MAGIC
# MAGIC
# MAGIC - ATOMICITY
# MAGIC - CONSISTENCY 
# MAGIC - ISOLATION
# MAGIC - DURABILITY
# MAGIC
# MAGIC
# MAGIC **ATOMICITY**
# MAGIC
# MAGIC - Atomicity means a transaction succeeds completely or doesn't get committed.
# MAGIC - Suppose we're updating 1,000 orders.
# MAGIC - We don't want a failed transaction to leave only half of its changes committed.
# MAGIC - Delta ensures the transaction commits atomically.
# MAGIC
# MAGIC **CONSISTENCY**
# MAGIC
# MAGIC - Consistency Means you Transactions Presenve the consistency gurantee enforced by the system
# MAGIC - For Example -: Delta Doesn't automatically Know all our busienss Rules
# MAGIC - If Quantity must be positive we still need appropriate validation or Constraint
# MAGIC
# MAGIC
# MAGIC **ISOLATION**
# MAGIC - Isolation Deals with Concurrent/Parallel/Simultaneuos Operations
# MAGIC - imagine one pipeline is writing new orders while another pipeline is reading the table
# MAGIC
# MAGIC **DURABILITY**
# MAGIC - If one Transaction is successfully Commited that means its changes are persisted.
# MAGIC - A notebook session editing doent undo a commited delta transaction
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC