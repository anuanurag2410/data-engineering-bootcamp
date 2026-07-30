# Databricks notebook source
# MAGIC %md
# MAGIC ### Variables and Data Types in Data Engineering
# MAGIC
# MAGIC - What is Variable? 
# MAGIC - How Variables store the Pipeline Information? 
# MAGIC - Different Data types in Python? 
# MAGIC - How to check the Data Type of a value in Python?
# MAGIC - Rules of Choosing Variable Names
# MAGIC - Common Mistakes by Beginners

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business scenario
# MAGIC
# MAGIC If i have some customer data to be loaded from an API 
# MAGIC
# MAGIC _There are some bunch information that the pipleine has to store_
# MAGIC
# MAGIC - **Pipeline Name:** customer_daily_pipeline
# MAGIC - **Source System:** customer_api
# MAGIC - **Source Records:** 1500
# MAGIC - **Target Records:** 1475
# MAGIC - **Load Duration:** 24.6 seconds
# MAGIC - **Pipeline Successful:** False
# MAGIC - **Error Message:** No error available
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is a Variable?
# MAGIC
# MAGIC A variable is a named space or location where you can use to store the value or data
# MAGIC
# MAGIC Source_Records = 1500
# MAGIC
# MAGIC Here **Source_Records** is the Variable and **1500** is the Data or Value and **=** is Assignment Operator

# COMMAND ----------

Source_Records = 1222

print("The Source Row Count is:",Source_Records)
print("The Source Row Count is:",Source_Records)
print("The Source Row Count is:",Source_Records)
print("The Source Row Count is:",Source_Records)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Variables Are Important in Data Engineering
# MAGIC
# MAGIC In Data Engineering there may be seeveral things that a Pipeline has to store-: 
# MAGIC
# MAGIC - Pipeline Name 
# MAGIC - Source System Name 
# MAGIC - File Path 
# MAGIC - Table Name 
# MAGIC - Record Count
# MAGIC - Execution Time 
# MAGIC - Pipeline Status
# MAGIC - Load Type

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Data Types for Data Engineers
# MAGIC
# MAGIC The main Data types in Pythomn used in this -: 
# MAGIC
# MAGIC - String
# MAGIC - Integer
# MAGIC - Float
# MAGIC - Boolean
# MAGIC - None
# MAGIC
# MAGIC **String Data Type**
# MAGIC
# MAGIC String will represent a Text adn it has to be enclosed in single or double Quotes

# COMMAND ----------

#Example 
Name='Anurag'
Pipeline_Name="Customer_pipeline_daily_Run"
file_Name="Customer_202607.csv"

print("Name of the Customer is :",Name)
print(f"Name of the Pipeline is : {Pipeline_Name} and it got loaded from File {file_Name}")

# COMMAND ----------

#Example:
source_path = "/data/landing/customers.csv"
target_table = "silver_customers"
pipeline_status = "RUNNING"


# COMMAND ----------

# MAGIC %md
# MAGIC **Integer Data Type** 
# MAGIC
# MAGIC It represent your **whole number**
# MAGIC
# MAGIC Examples-: 
# MAGIC
# MAGIC source_count=100
# MAGIC

# COMMAND ----------

source_count=1500
target_count=200
id=2

#Example:
total_files = 10
processed_files = 8
failed_files = 2

print(f"The Soruce Row Count is {source_count}")
print(f"The Target Row COunt is {target_count}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Float Data Type
# MAGIC Float Represents the number containing decimal points
# MAGIC
# MAGIC Examples
# MAGIC - load_duration = 56.4
# MAGIC - fileInMB=456.66

# COMMAND ----------

load_duration = 56.4
fileInMB=456.66

# COMMAND ----------

# DBTITLE 1,Float Data Type
source_count = 1500
valid_count = 1475

data_quality_percentage = valid_count / source_count * 100

print(f"The Data Quality Percentage is {data_quality_percentage:.3f}%")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Boolean Data Type
# MAGIC
# MAGIC The Boolean value has only tow type of data that is **True and False**

# COMMAND ----------

# DBTITLE 1,Boolean Data Type
pipeline_success=True
pipeline_failure=False
#Example
print(f"Pipeline Status is {pipeline_success}")
print(f"Pipeline Status is {pipeline_failure}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### None Data Type 
# MAGIC None represents the absence of a value.
# MAGIC
# MAGIC It means that a value is currently unavailable, missing or not assigned.
# MAGIC

# COMMAND ----------

pipeline_status=None
print(f"Status of the Pipeline is {pipeline_status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Type Check in Python

# COMMAND ----------

source_count = 1500
valid_count = 1475
pipeline_success=True
pipeline_failure=False
duration=56.66
Pipeline_name='Customer_ingestion_pipeline'
pipeline_status=None

# COMMAND ----------

print(type(pipeline_status))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python is Dynamically Typed langauge
# MAGIC
# MAGIC This means we do not have to declare the data type explicitly before creating a variable.

# COMMAND ----------

Row_count=10.99

print(type(Row_count))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variable Naming Rules
# MAGIC
# MAGIC **Rule 1:** A variable can contain letters, numbers and underscores
# MAGIC
# MAGIC

# COMMAND ----------

source_count=100
soruce_cont_1=100
pipeline_name = "orders_pipeline"

# COMMAND ----------

# MAGIC %md
# MAGIC **Rule 2:** A variable cannot start with a number
# MAGIC

# COMMAND ----------

# 2Source_count=10

Source_count2=10
Source_count_2=10

# COMMAND ----------

# MAGIC %md
# MAGIC **Rule 3:** Spaces are not allowed

# COMMAND ----------

Source_Count=100

# COMMAND ----------

# MAGIC %md
# MAGIC **Rule 4:** Special characters are generally not allowed
# MAGIC

# COMMAND ----------

#Invalid
source-count=100
soruce@count=100

#valid
source_count=100
_source_count=100
sorunceCount_=100

# COMMAND ----------

# MAGIC %md
# MAGIC **Rule 5:** Variable names are case-sensitive

# COMMAND ----------

source_count=100
SOURCE_COUNT=15000
print(f"This is Small Letter {source_count}")
print(f"This is CAPITAL Letter {SOURCE_COUNT}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Rule 6:** Python keywords cannot be used as variable names
# MAGIC
# MAGIC Avoid variable names such as:
# MAGIC - if
# MAGIC - for
# MAGIC - class
# MAGIC - return
# MAGIC - True
# MAGIC - False
# MAGIC - None
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Meaningful Variable Names
# MAGIC

# COMMAND ----------

# DBTITLE 1,meaningful Variable name
a=100
source_count=100
Source_count=100
SourceCount=100
SOURCECOUNT=100
sourcecount=100

# COMMAND ----------

# MAGIC %md

# COMMAND ----------


