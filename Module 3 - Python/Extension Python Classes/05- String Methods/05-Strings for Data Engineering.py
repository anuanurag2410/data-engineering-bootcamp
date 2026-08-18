# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Strings for Data Engineering
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### What Is a String?
# MAGIC **A string is a sequence of characters.**
# MAGIC
# MAGIC A character can be:
# MAGIC - Letter
# MAGIC - Number
# MAGIC - Space
# MAGIC - Underscore
# MAGIC - Special symbol
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Scenario
# MAGIC
# MAGIC **file_name="customer_2026_08_18.csv"**
# MAGIC
# MAGIC From this one string, we may need to identify:
# MAGIC - Entity name
# MAGIC - Processing year
# MAGIC - Processing month
# MAGIC - Processing day
# MAGIC - File extension
# MAGIC - Whether the file is a valid CSV
# MAGIC - Whether the filename follows the expected pattern
# MAGIC
# MAGIC

# COMMAND ----------

column_name = "customer full name"
column_name="customer_full_name"

column_name='customer_full_name'

#Both are same

# COMMAND ----------

# DBTITLE 1,Multi Line String
pipeline_description="""
This pipeline 
records thje customer data 
validates it 
and also make the things checked in target
"""
print(pipeline_description)

# COMMAND ----------

# DBTITLE 1,String Length
file_name='customer_file.csv'
print(len(file_name))

# COMMAND ----------

# DBTITLE 1,Empty String
error_message=""
if len(error_message)==0:
    print("Its an Empty String")
else:
    print("Its not Empty ")

# COMMAND ----------

# DBTITLE 1,String Indexing
# List Concept
# a=[1,2,3,4]
# print(a[-2])


file_type="csv"
print(file_type[5])

# COMMAND ----------

# DBTITLE 1,String Slicing
#Silcing Allows to extract a postion or part of the string
#string[start:end]
#start is included, end is excluded

file_name="customer.csv"
extract=file_name[0:8]
print(extract)

# COMMAND ----------

file_name="customer.csv"
extract=file_name[:8]
print(extract)

# COMMAND ----------

file_name="customer.csv"
extract=file_name[9:]
print(extract)

# COMMAND ----------

file_name="customer.csv"
extract=file_name[-4:]
print(extract)

# COMMAND ----------

file_name="customer.csv"
extract=file_name[0:10:3]
print(extract)

# COMMAND ----------

# DBTITLE 1,List is mutable
a=[1,4,5,6,7]
a[3]=10
print(a)

# COMMAND ----------

# DBTITLE 1,String is Immutable
file_name="customer.csv"
file_name[0]='C'

# COMMAND ----------

file_name="customer.csv"
file_name="C"+file_name[1:]
print(file_name)

# COMMAND ----------

# DBTITLE 1,Lower
File_name="CUSTOMER_CSV.CSV"
clean_file=File_name.lower()
print(clean_file)

# COMMAND ----------

# DBTITLE 1,Upper
File_name="customer_csv.csv"
clean_file=File_name.upper()
print(clean_file)

# COMMAND ----------

# MAGIC %md
# MAGIC success
# MAGIC Successs
# MAGIC SUCEss
# MAGIC SUCCESS

# COMMAND ----------

print("SUCCESS"=="SUCCESS")

# COMMAND ----------

# DBTITLE 1,Strip Method
Name="  Anurag Srivastava "
print("BEFORE",len(Name))
Name=Name.strip()
print("AFTER",len(Name))

# COMMAND ----------

# DBTITLE 1,Replace Method
column_name="Customer-Full-Name"

clean_column_name=column_name.replace("-","_")
print(clean_column_name)

# COMMAND ----------

column_name = " Customer-Full Name "

clean_column_name=column_name.strip()
clean_column_name=clean_column_name.replace("-","_")
clean_column_name=clean_column_name.replace(" ","_")
print(clean_column_name)

# COMMAND ----------

# DBTITLE 1,method Chaining
column_name = " Customer-Full Name "

clean_column_name_chain=(
    column_name
    .strip()
    .lower()
    .replace("-","_")
    .replace(" ","_")
)
print(clean_column_name_chain)

# COMMAND ----------

# DBTITLE 1,Startswith Method
file_name = "Customer.csv"

is_customer_file=file_name.startswith("Customer")
print(is_customer_file)

# COMMAND ----------

file_name = "Customer.json"

is_supported_type=file_name.endswith((".csv",".json",".parquet"))
print(is_supported_type)

# COMMAND ----------

# DBTITLE 1,Split Method
file="The name of Customer is Anurag"
split_details=file.split(" ")
print(split_details)

# COMMAND ----------

file="Customer_2026_08_18"
file_details=file.split("_")
print(f"File Type is : {file_details[0]}")
print(f"Year is : {file_details[1]}")
print(f"Month is : {file_details[2]}")


# COMMAND ----------


