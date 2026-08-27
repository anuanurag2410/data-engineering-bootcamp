# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Lists for Data Engineering

# COMMAND ----------

file_1='customer.csv'
file_2='orders.csv'
file_3='products.csv'

# COMMAND ----------

received_files=[

'customer.csv','orders.csv','products.csv'

]
print(type(received_files))

# COMMAND ----------

a=[]

print(type(a)  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importance of List in Data Engineering

# COMMAND ----------

received_files=[
'customer.csv',
'orders.csv',
'products.csv'

]

# COMMAND ----------

required_columns=[
'customer_Id',
'Cusotmer_name',
'email'
]

# COMMAND ----------

pipileine_status=[
"SUCCESS",
"INPROGRESS",
"FAILED"

]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lists can store Multiple Type of Data

# COMMAND ----------

Pipeline_Information=[
'Customer_pipeine',
1500,
"Success",
'2026-01-01',
False
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexing in List or Accessing the Elements in List

# COMMAND ----------

received_files=[
'customer.csv',
'orders.csv',
'products.csv'
]

# COMMAND ----------

received_files[-1]

# COMMAND ----------

received_files[-2]

# COMMAND ----------

received_files[2]='Payments.json'
print(received_files)

# COMMAND ----------

print("Total Files Received is ",len(received_files))

# COMMAND ----------

l=len(received_files)
print(f"The Number of Files Received are -: {l}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lists Slicing

# COMMAND ----------

received_files=[
'customer.csv',
'orders.csv',
'products.csv',
'Payments.csv'
]

# COMMAND ----------

print(received_files[0:2])

# COMMAND ----------

print(received_files[0:3:2])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lists Functions

# COMMAND ----------

processed_files=[]

# COMMAND ----------

processed_files.append("customer3.csv")
print(processed_files)

# COMMAND ----------

# DBTITLE 1,Add List to Another List
received_files=[
'customer.csv',
'orders.csv',
'products.csv',
'Payments.csv'
]

New_files=["Cusotmer_NewFile.csv","Orders_New_File.csv"]

New_files.extend(received_files)
print(New_files)

# COMMAND ----------

# DBTITLE 1,Insert at specific Place
received_files=[
'customer.csv',
'products.csv',
'Payments.csv'
]

received_files.insert(1,"orders.csv")
print(received_files)


# COMMAND ----------

# DBTITLE 1,Remove some Elements From List
received_files=['customer.csv', 'orders.csv', 'products.csv', 'Payments.csv',"Test.csv"]

received_files.remove("Test.csv")
print(received_files)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


