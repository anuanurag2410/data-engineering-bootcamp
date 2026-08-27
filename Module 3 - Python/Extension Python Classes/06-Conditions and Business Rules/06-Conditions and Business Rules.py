# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### IF Statement

# COMMAND ----------

order_amount=2500

# COMMAND ----------

if order_amount>=0:
    print("Order Amount is Valid")
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### IF and Else Statement

# COMMAND ----------

order_amount=-2500
if order_amount>=0:
    print("Order Amount is Valid")
else:
    print("Order Amount is Invalid")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Engineering Aspects

# COMMAND ----------

customer_id=111

# COMMAND ----------

# MAGIC %md
# MAGIC Business Rule 
# MAGIC
# MAGIC - If the Customer Id is not valid reject the value

# COMMAND ----------

if customer_id is None:
    print("Value is Rejected🚨")

else:
    print("Customer Id has been Accepted✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Elif Statement

# COMMAND ----------

# MAGIC %md
# MAGIC Business rule:
# MAGIC
# MAGIC
# MAGIC 0% rejected
# MAGIC → SUCCESS
# MAGIC
# MAGIC
# MAGIC Up to 5% rejected
# MAGIC → PARTIAL_SUCCESS
# MAGIC
# MAGIC
# MAGIC More than 5%
# MAGIC → FAILED
# MAGIC

# COMMAND ----------

rejected_percentage=3.5

# COMMAND ----------

if rejected_percentage==0: 
    print("The Pipeline Data is Successfully Loaded✅")

elif rejected_percentage>0 and rejected_percentage<5:
    print("The Pipeline Data is Partially Loaded 🔥")
    
else:
    print("The Pipeline Data is Rejected🚨")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Invalid Data

# COMMAND ----------

# DBTITLE 1,Business Rule of minimum product amount 200rs
order_amount=100

if order_amount>=200:
    print("Valid Shopping Entry")

else:
    print("Invalid Shopping Entry")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nested Conditions

# COMMAND ----------

file_received=False
Schema_valid=True

if file_received:
    if Schema_valid:
        print("Start Processign the Data")

    else:
        print("Schema is not Valid")
else:
    print("File is not Received")

# COMMAND ----------

file_received=False
Schema_valid=True

if file_received and Schema_valid:
    print("Start Processign the Data")

else:
    print("File is not Received")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Truthly and Falsy Values

# COMMAND ----------

# MAGIC %md
# MAGIC - None
# MAGIC - False
# MAGIC - 0
# MAGIC - ""
# MAGIC - []
# MAGIC - {}

# COMMAND ----------

customer_name =' '

if customer_name:
    print("It is Avaliable")

else:
    print("Missing Customer Name")
  

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------



# COMMAND ----------


