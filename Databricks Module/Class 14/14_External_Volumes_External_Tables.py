# Databricks notebook source
# MAGIC %md
# MAGIC # External Volume vs External Table: When to Use Which?

# COMMAND ----------

# MAGIC %md
# MAGIC **External Volume**
# MAGIC
# MAGIC - An External Volume is a unity catalog object that provides govered access to files stored in an external cloud storage location
# MAGIC - The files remin in our cloud storage but we access them through the unity catalog volume 
# MAGIC - Way to access or read it -: **/Volumes/dataxdevelopment_1/default/dataxexternalvolume/OrdersFiles/orders.csv**
# MAGIC
# MAGIC **External Table**
# MAGIC - An external table is a registered table whoese data files are sotred at an external cloud storage location 
# MAGIC - Unity catalog manages the table metadata and permissions 
# MAGIC - But the external storage location and underlying files remins seperately managed by cloud provider 
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text("catalog", "dataxdevelopment_1", "Unity Catalog")
catalog = dbutils.widgets.get("catalog")
if not catalog:
    raise ValueError("Parameter 'catalog' is required")
spark.sql(f"USE CATALOG `{catalog}`")
print(f"Using catalog: {catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS ${catalog}.bronze.orders_external
# MAGIC LOCATION 'abfss://dev-datax@dataxazure.dfs.core.windows.net/Volume_External/OrderData'
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW VOLUMES IN ${catalog}.bronze;

# COMMAND ----------

# DBTITLE 1,Read the Orders.csv
volume_path =f"/Volumes/{catalog}/bronze/orders_external/"
display(dbutils.fs.ls(volume_path))

# COMMAND ----------

order_path=f'/Volumes/{catalog}/bronze/orders_external/orders.csv'

orders_df=spark.read.option('header','true').option('inferSchema','true').csv(order_path)
display(orders_df)

# COMMAND ----------

# DBTITLE 1,EXTERNAL TABLE
# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.OrderData_ExternalTable(
# MAGIC    order_id INT,
# MAGIC     customer_id STRING,
# MAGIC     product_id STRING,
# MAGIC     order_date DATE,
# MAGIC     quantity INT,
# MAGIC     unit_price DECIMAL(10,2),
# MAGIC     order_status STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://dev-datax@dataxazure.dfs.core.windows.net/landing_v3/OrderData_ExternalTable'
# MAGIC

# COMMAND ----------

orders_df.write.mode('append').saveAsTable(f'{catalog}.bronze.OrderData_ExternalTable')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE ${catalog}.bronze.OrderData_ExternalTable

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.OrderData_ExternalTable(
# MAGIC    order_id INT,
# MAGIC     customer_id STRING,
# MAGIC     product_id STRING,
# MAGIC     order_date DATE,
# MAGIC     quantity INT,
# MAGIC     unit_price DECIMAL(10,2),
# MAGIC     order_status STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://dev-datax@dataxazure.dfs.core.windows.net/landing_v3/OrderData_ExternalTable'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from ${catalog}.bronze.OrderData_ExternalTable

# COMMAND ----------

# MAGIC %md
# MAGIC # Production Scenario: When Should We Use External Tables?
# MAGIC
# MAGIC - Existing Delta data must remain at a specified cloud storage path.
# MAGIC - Another system or team manages the storage lifecycle.
# MAGIC - An organization has specific storage ownership requirements.
# MAGIC - A migration needs to preserve an existing external dataset location.
# MAGIC
# MAGIC
# MAGIC # When managed Tables are Better?
# MAGIC
# MAGIC - Databricks owns the complete data lifecysle 
# MAGIC - WE want databricks managed storage and optimizations 
# MAGIC - we dont have a requirement to control the phycical table path
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

