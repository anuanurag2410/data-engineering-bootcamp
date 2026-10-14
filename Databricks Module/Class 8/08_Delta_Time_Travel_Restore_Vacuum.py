# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08_Delta_Time_Travel_Restore_Vacuum

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE datax_db.orders_time_travel_demo 
# MAGIC AS SELECT * from datax_db.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe Detail datax_db.orders_time_travel_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe extended datax_db.orders_time_travel_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY datax_db.orders_time_travel_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO datax_db.orders_time_travel_demo
# MAGIC (
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     product_id,
# MAGIC     order_date,
# MAGIC     quantity,
# MAGIC     unit_price,
# MAGIC     order_status
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC     1011,
# MAGIC     'C108',
# MAGIC     'P101',
# MAGIC     '2026-09-06',
# MAGIC     1,
# MAGIC     899.00,
# MAGIC     'PENDING'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY datax_db.orders_time_travel_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE datax_db.orders_time_travel_demo
# MAGIC SET order_status = 'COMPLETED'
# MAGIC WHERE order_id = 1011;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY datax_db.orders_time_travel_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM datax_db.orders_time_travel_demo
# MAGIC WHERE order_id = 1011;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY datax_db.orders_time_travel_demo

# COMMAND ----------


df = spark.table("datax_db.orders_time_travel_demo")
df.write.format("delta").mode("overwrite").save("/Volumes/dataxdevelopment_7405619193321738/default/delta_datax")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY datax_db.orders_time_travel_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE datax_db.orders_time_travel_demo TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from datax_db.orders_time_travel_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from datax_db.orders_time_travel_demo version as of 5

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from datax_db.orders_time_travel_demo version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from datax_db.orders_time_travel_demo 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY datax_db.orders_time_travel_demo

# COMMAND ----------

# MAGIC %md
# MAGIC # VACUUM
# MAGIC
# MAGIC - Vacuum is used to remove the data files that are no longer needed by the current delta tables and are older than specified rentention threshold.
# MAGIC - VACUUM helps clean up eligible files 

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM datax_db.orders_time_travel_demo
# MAGIC RETAIN 1 HOURS DRY RUN;
# MAGIC

# COMMAND ----------


df = spark.table("datax_db.orders_time_travel_demo")
df.write.format("delta").mode("overwrite").save("/Volumes/dataxdevelopment_7405619193321738/default/delta_datax")