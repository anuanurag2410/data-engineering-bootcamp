# Databricks notebook source
dbutils.widgets.text("source_path", "", "source_path")
source_path = dbutils.widgets.get("source_path")

dbutils.widgets.text("bronze_table", "", "bronze_table")
bronze_table = dbutils.widgets.get("bronze_table")

# COMMAND ----------

print(f"Reading the data from --> {source_path}")
print(f"Target Table Name is --> {bronze_table}")

# COMMAND ----------

#read the Json
df=spark.read.json(source_path)

# COMMAND ----------

#Write to the bronze table 

df.write.mode("overwrite").saveAsTable(bronze_table)

print(f"Data written to the table {bronze_table}")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from ${bronze_table}