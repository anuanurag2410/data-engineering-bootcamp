# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Class 4 - Different Type of Files and Data

# COMMAND ----------

# MAGIC %md
# MAGIC **What is Data?**
# MAGIC
# MAGIC
# MAGIC - CSV
# MAGIC - Parquet Files 
# MAGIC - JSON Files 
# MAGIC - Databases 
# MAGIC - APIs
# MAGIC - Cloud Storage
# MAGIC - Streaming Systems
# MAGIC
# MAGIC **_SPARK IS NOT A STORAGE SYSTEM_**

# COMMAND ----------

# MAGIC %md
# MAGIC # What is File ?
# MAGIC
# MAGIC A file is the data that is physically present somewhere(Pysical Storage entity)
# MAGIC
# MAGIC **Examples**
# MAGIC - orders.csv
# MAGIC - customers.csv
# MAGIC - prodcuts.csv
# MAGIC - prodcuts.json
# MAGIC - payments.xml
# MAGIC - students.parquet
# MAGIC
# MAGIC # What is a Dataframe?
# MAGIC
# MAGIC - DF is a Spark's Structured representation of data
# MAGIC - Spark uses it while procesing the data
# MAGIC - It is in rows and columns format
# MAGIC - we perform transformations on it.
# MAGIC - It is **not atuotmatically permanent storage**

# COMMAND ----------

# MAGIC %md
# MAGIC - Creating a DataFrame does not automatically create a table.
# MAGIC - Creating a DataFrame does not automatically persist your transformed data.
# MAGIC

# COMMAND ----------

orders_path="/Workspace/Users/anuanurag2410@gmail.com/data-engineering-bootcamp/Databricks Module/Class 4/orders.csv"

orders_df=spark.read.option("header",True).option("inferSchema",True).csv(orders_path)
display(orders_df)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Prove that DF and Files are not SAME**

# COMMAND ----------

type(orders_df)

# COMMAND ----------

orders_df.count()

# COMMAND ----------

orders_df.select("order_id","customer_id","order_status").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why CSV is not IDEAL for EVERYTHING
# MAGIC
# MAGIC - Text-based Format
# MAGIC - Doesn't Preseve Strongly any schema itself
# MAGIC - Large storge compared with efficient coloumnar formats in many workloads
# MAGIC - Analytic Engines ofned need to parse data more

# COMMAND ----------

# MAGIC %md
# MAGIC # Introducing PAQRUET
# MAGIC
# MAGIC - Parquet is a columnar file format.
# MAGIC - Instead of thinking about the data only row-by-row, it stores data in a way optimized for column-oriented analytical processing.
# MAGIC - It preserves schema information.
# MAGIC - It supports efficient compression.
# MAGIC - It works very well with Spark.
# MAGIC

# COMMAND ----------

parquet_path='/Volumes/dataxdevelopment_7405608929107032/default/datax'


orders_df.write \
    .mode("overwrite") \
    .parquet(parquet_path)


# COMMAND ----------

parquet_df=spark.read.parquet("/Volumes/dataxdevelopment_7405608929107032/default/datax")
parquet_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # INTRODUCTING DELTA 

# COMMAND ----------

# MAGIC %md
# MAGIC **What is Delta?**
# MAGIC
# MAGIC - Delta Lake is a storage layer commonly used in Databricks
# MAGIC - Delta data uses the Parquet Data plus the Transaction Metadata
# MAGIC - This enables the capabilities that plain **Parquet** doesn't have or provide
# MAGIC
# MAGIC _The properties that DELTA Provides_
# MAGIC
# MAGIC - ACID
# MAGIC - Transaction Logs
# MAGIC - Time Travel 
# MAGIC - RESTORE 
# MAGIC - MERGE

# COMMAND ----------

delta_path="/Volumes/dataxdevelopment_7405608929107032/default/delta_datax"
orders_df.write.format("delta").mode('overwrite').save(delta_path)
orders_df.write.format("delta").mode('overwrite').saveAsTable('default.orders_delta')

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

