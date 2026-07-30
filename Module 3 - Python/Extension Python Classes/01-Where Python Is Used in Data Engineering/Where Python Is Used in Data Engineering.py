# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### The Python we are going to learn is specifically for Data Engineering and not for Programming

# COMMAND ----------

# MAGIC %md
# MAGIC - Read Data
# MAGIC - Validate the data
# MAGIC - Clean Data
# MAGIC - Automate repetetive Task or work 
# MAGIC - Call APIs 
# MAGIC - Process Files 
# MAGIC - Connect Databases 
# MAGIC - Build Data Pipelines
# MAGIC - Handle pipeline faliures

# COMMAND ----------

# MAGIC %md
# MAGIC ### What Does a Data Engineer Do with Python?

# COMMAND ----------

# MAGIC %md
# MAGIC - Source System
# MAGIC -      ↓
# MAGIC - Extract Data
# MAGIC -      ↓
# MAGIC - Validate Data
# MAGIC -      ↓
# MAGIC - Transform Data
# MAGIC -      ↓
# MAGIC - Load Data
# MAGIC -      ↓
# MAGIC - Generate Logs and Alerts
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python For Data Extraction

# COMMAND ----------

import requests

response=requests.get("https//apilink.exmple.com",timeout=30)
customers=response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python for File Processing
# MAGIC
# MAGIC A data engineer will be responsible if you get say 50 files everyday to check it 
# MAGIC
# MAGIC A data engineer will -: 
# MAGIC
# MAGIC - Find the Files 
# MAGIC - Check whether the file is empty or not 
# MAGIC - check thier extensions
# MAGIC - Read the records 
# MAGIC - Move the Processed files 
# MAGIC - Reject the invalid files

# COMMAND ----------

#for Example 
file_name="customer_30_jul.csv"
if file_name.endswith(".csv"):
    print("File is csv")
else:
    print("File is not csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python for Data Validation
# MAGIC
# MAGIC Scenario is --> There is a source system that sends 1000 records daily , you lodad the data to the target and see that only 950 rows are there in target 
# MAGIC
# MAGIC _A **Data Engineer** will be responsible to validate the data and find the differences_
# MAGIC
# MAGIC

# COMMAND ----------

source_count=1000
target_count=950 

rejected_count=source_count-target_count
print(rejected_count)


# COMMAND ----------

# MAGIC %md
# MAGIC
