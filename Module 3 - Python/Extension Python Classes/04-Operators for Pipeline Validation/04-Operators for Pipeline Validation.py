# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Business Scenario
# MAGIC %md
# MAGIC Suppose we have a daily customer pipeline.
# MAGIC
# MAGIC The source system sends one file.
# MAGIC
# MAGIC That file contains customer records.
# MAGIC
# MAGIC We want to validate the following:
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - Source records: 1500
# MAGIC - Target records: 1475
# MAGIC - File name: customers_2026_07_31.csv
# MAGIC - File received: True
# MAGIC - Schema valid: True
# MAGIC - Error message: None
# MAGIC - Maximum allowed rejection percentage: 5%
# MAGIC
# MAGIC **Based on these values, we need to answer:**
# MAGIC - How many records were rejected?
# MAGIC - What is the rejection percentage?
# MAGIC - Do source and target counts match?
# MAGIC - Is the file format valid?
# MAGIC - Is the schema valid?
# MAGIC - Is there any error?
# MAGIC - Should the pipeline be marked successful
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### What Is an Operator?
# MAGIC Operator is a Symbol or keyword used to perform some calculations on one or more values

# COMMAND ----------

Source_records=1500
Target_records=1475

#using Arthemetic Operator
rejected_records=Source_records-Target_records
print(rejected_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type of Operators
# MAGIC
# MAGIC - Arithemetic Operator 
# MAGIC - Comparison Operator
# MAGIC - logical Operator
# MAGIC - Assignment Operator
# MAGIC - Membership Operator
# MAGIC - Identity Operator
# MAGIC - Operator Operator

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Pipeline Variables
# MAGIC

# COMMAND ----------

pipeline_name = "customer_daily_pipeline"

source_count = 1500
target_count = 1475

file_name = "customers_2026_07_31.csv"
file_received = True
schema_valid = True

error_message = None
maximum_rejection_percentage = 5


# COMMAND ----------

# MAGIC %md
# MAGIC ### Arithmetic Operators
# MAGIC It is used to perform arthemetic calculations
# MAGIC
# MAGIC The main arithmetic operators are:
# MAGIC -  +   Addition
# MAGIC -  -   Subtraction
# MAGIC -  *   Multiplication
# MAGIC - /   Division
# MAGIC - //  Floor division
# MAGIC - %   Modulus
# MAGIC - **  Exponent
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Subtraction
source_count = 1500
target_count = 1475

rejected_count = source_count - target_count

print(rejected_count)


# COMMAND ----------

# DBTITLE 1,Addition
file_1_count = 800
file_2_count = 700

total_source_count = file_1_count + file_2_count

print(total_source_count)


# COMMAND ----------

# DBTITLE 1,Multiplication
records_per_batch = 500
number_of_batches = 4

total_capacity = records_per_batch * number_of_batches

print(total_capacity)


# COMMAND ----------

# DBTITLE 1,Division
rejected_count = 25
source_count = 1500

rejection_percentage = rejected_count / source_count * 100

print(rejection_percentage)


# COMMAND ----------

# DBTITLE 1,Floor Division Operator
total_records = 1500
batch_size = 200

complete_batches = total_records // batch_size

print(complete_batches)


# COMMAND ----------

# DBTITLE 1,Modulus
total_records = 1500
batch_size = 200

remaining_records = total_records % batch_size

print(remaining_records)


# COMMAND ----------

# DBTITLE 1,Exponent
2**5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparison Operators
# MAGIC
# MAGIC - ==   Equal to
# MAGIC - !=   Not equal to
# MAGIC - >    Greater than
# MAGIC - <    Less than
# MAGIC - >=   Greater than or equal to
# MAGIC - <=   Less than or equal to
# MAGIC

# COMMAND ----------

# DBTITLE 1,Equal-To Operator
source_count = 1500
target_count = 1475

counts_match = source_count == target_count

print(counts_match)


# COMMAND ----------

# DBTITLE 1,Important Difference Between = and ==
source_count = 1500
source_count == target_count
# =   assign a value
# ==  compare two values


# COMMAND ----------

# DBTITLE 1,Not-Equal Operator
counts_different = source_count != target_count

print(counts_different)


# COMMAND ----------

# DBTITLE 1,Greater-Than and Less-Than Operators
rejection_percentage = 1.67
maximum_rejection_percentage = 5

within_limit = rejection_percentage < maximum_rejection_percentage

print(within_limit)



# COMMAND ----------

rejection_percentage=8.5
within_limit = rejection_percentage < maximum_rejection_percentage
print(within_limit)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logical Operators
# MAGIC
# MAGIC - and 
# MAGIC - or 
# MAGIC - not

# COMMAND ----------

# MAGIC %md
# MAGIC ### and operator
# MAGIC
# MAGIC Suppose a pipeline should succeed only when:
# MAGIC
# MAGIC - File is received
# MAGIC - Schema is valid
# MAGIC - Rejection percentage is within the limit
# MAGIC - No error exists
# MAGIC

# COMMAND ----------

file_received=True
Schema_valid=True

basic_validation_pass=file_received and Schema_valid
print(basic_validation_pass)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OR Operator
# MAGIC
# MAGIC The or operator returns True when at least one condition is True.
# MAGIC
# MAGIC Suppose we support CSV or JSON files.
# MAGIC

# COMMAND ----------

file_name="cusotmer.json"
is_supported= file_name.endswith(".csv") or file_name.endswith(".json")
print(is_supported)

# COMMAND ----------

# MAGIC %md
# MAGIC ### not Operator
# MAGIC The not operator reverses a Boolean result.
# MAGIC

# COMMAND ----------

file_received=False
file_missing=not file_received
print(file_missing)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combining Multiple Logical Conditions
# MAGIC ### 

# COMMAND ----------

file_received = True
schema_valid = True
error_message = None
rejection_percentage = 1.67
maximum_rejection_percentage = 5

# Suppose a pipeline should succeed only when:

# - File is received
# - Schema is valid
# - Rejection percentage is within the limit
# - No error exists

# COMMAND ----------

pipeline_successful=(
file_received 
and schema_valid 
and error_message is None 
and rejection_percentage <=maximum_rejection_percentage)
print(pipeline_successful)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assignment Operators
# MAGIC
# MAGIC _Assignment operators are used to assign or update values.
# MAGIC The basic assignment operator is:
# MAGIC =_
# MAGIC
# MAGIC
# MAGIC - +=
# MAGIC - -=
# MAGIC - *=
# MAGIC - /=

# COMMAND ----------

a=10
b=20

print("Before",b)
b+=10 # b= b+10
print(a)
print("After",b)

# COMMAND ----------

a=10
b=20

print("Before",b)
b*=10 # b= b*10
print(a)
print("After",b)

# COMMAND ----------

a=10
b=20

print("Before",b)
b/=10 # b= b/10
print(a)
print("After",b)

# COMMAND ----------

a=10
b=20

print("Before",b)
b-=10 # b= b-10
print(a)
print("After",b)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Membership Operators
# MAGIC
# MAGIC Membership operators check whether a value exists inside another collection or sequence.
# MAGIC
# MAGIC _The main membership operators are:_
# MAGIC - in
# MAGIC - not in
# MAGIC

# COMMAND ----------

file_name="customer_20260818.csv"

is_csv=".csv" in file_name
print(is_csv)

# COMMAND ----------

file_name="customer_20260818.csv.backup"

is_csv=".csv" in file_name
print(is_csv)

print(file_name.endswith(".backup"))


# COMMAND ----------

supported_formats=[".json",'.csv','.txt',".tsv",'.parquet']
file_extension=".csv"

is_supported_format=file_extension in supported_formats
print(is_supported_format)

# COMMAND ----------

available_columns = [
    "customer_id",
    "customer_name",
    "email"
]

column_missing= "order_amount" not in available_columns
print(column_missing)

# COMMAND ----------

# MAGIC %md
# MAGIC Identity Operator 
# MAGIC
# MAGIC - is 
# MAGIC - is not

# COMMAND ----------

error_message="File Not Found"

if error_message is  None:
    print("No error in Pipeline")


else:
    print("Error in Pipeline -: ",error_message)




# COMMAND ----------

error_message=None

if error_message ==  None:
    print("No error in Pipeline")


else:
    print("Error in Pipeline -: ",error_message)




# COMMAND ----------

# MAGIC %md
# MAGIC ### == versus is

# COMMAND ----------

value1=[1,2]
value2=[1,2]

print(value1==value2)
print(value1 is value2)

# COMMAND ----------

==

is None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operator Precedence
# MAGIC

# COMMAND ----------

result=10+5*2
# 5*2 
# 10 +10
#20
print(result)

# COMMAND ----------

result=(10+5)*2

print(result)

# COMMAND ----------

rejection_percentage=(rejected_count/source_count)*100


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
