# Databricks notebook source
# MAGIC %md
# MAGIC **Suppose we receive this customer record from an API or a CSV file:**

# COMMAND ----------

customer_record = {
    "customer_id": "101",
    "customer_name": "Anurag",
    "order_amount": "2500.50",
    "is_active": "True",
    "discount": "",
    "last_order_date": None
}


# COMMAND ----------

print(type(customer_record['customer_id']))
print(type(customer_record['customer_name']))
print(type(customer_record['order_amount']))
print(type(customer_record['is_active']))
print(type(customer_record['discount']))
print(type(customer_record['last_order_date']))

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is Type Conversion?
# MAGIC
# MAGIC Type Conversion means changing a value from one data type into another data type as per the requirement

# COMMAND ----------

# DBTITLE 1,Type Conversion from Str to int
customer_id="101"
customer_id=int(customer_id)
print(type(customer_id))
print(customer_id)

# COMMAND ----------

# MAGIC %md
# MAGIC Python provides build in functions for type conversion-:
# MAGIC
# MAGIC - int()
# MAGIC - float()
# MAGIC - str()
# MAGIC - bool()

# COMMAND ----------

# MAGIC %md
# MAGIC ### String to Int Conversion

# COMMAND ----------

# DBTITLE 1,Done Type Casting to int
source_count="1500"
print(type(source_count))
source_count=int(source_count)


# COMMAND ----------

a=10
b=source_count+a
print(b)

# COMMAND ----------

#Limitatons

source_count="10.10"
source_count=float(source_count)
print(source_count)
print(type(source_count))

# COMMAND ----------

source_count=int(source_count)
print(source_count)
print(type(source_count))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converting String to Float

# COMMAND ----------

order_amount = "2500.50"
print(type(order_amount))
order_amount=float(order_amount)
print(type(order_amount))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converting Values to String
# MAGIC

# COMMAND ----------

# DBTITLE 1,Without Type Conversion
batch_id=101

#customer_batch_101.csv

file_name="customer_batch_"+batch_id+".csv"
print(file_name)

# COMMAND ----------

# DBTITLE 1,With Type Conversion
batch_id=501

#customer_batch_101.csv

file_name="customer_batch_"+str(batch_id)+".csv"

print(file_name)

# COMMAND ----------

# DBTITLE 1,With Type Conversion Alternate
batch_id=5102

#customer_batch_101.csv

file_name=f"customer_batch_{batch_id}.csv"

print(file_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Boolean Conversion
# MAGIC

# COMMAND ----------

# DBTITLE 1,True value in bool
is_active="True"
print(type(is_active))

is_active=bool(is_active)
print(type(is_active))
print(is_active)

# COMMAND ----------

# DBTITLE 1,False value in bool
is_active="False"
print(type(is_active))

is_active=bool(is_active)
print(type(is_active))
print(is_active)

#produces incorrect result directly if we try to type convert the string to boolean

# COMMAND ----------

# MAGIC %md
# MAGIC ### Safe String-to-Boolean Conversion
# MAGIC

# COMMAND ----------

is_active="True"
is_active=is_active.lower()=="true"
print(is_active)
print(type(is_active))

# COMMAND ----------

is_active="False"
is_active=is_active.lower()=="true"
print(is_active)
print(type(is_active))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Different Boolean Formats
# MAGIC
# MAGIC - TRUE
# MAGIC - FALSE
# MAGIC - True
# MAGIC - False
# MAGIC - Yes 
# MAGIC - No 
# MAGIC - Y
# MAGIC - N 
# MAGIC - 1
# MAGIC - 0
# MAGIC

# COMMAND ----------

source_value="Y"
is_active=source_value.strip().lower() in ["true",'yes','y','1']
print(is_active)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing Value
# MAGIC
# MAGIC - discount= "" 
# MAGIC - discount= None
# MAGIC - discount= "Null"

# COMMAND ----------

value_1=None
value_2=""
value_3="NULL"
value_4="null"
value_5="N"
value_6="No"
value_7=0
value_8=False

print(type(value_1))
print(type(value_2))
print(type(value_3))
print(type(value_4))
print(type(value_5))
print(type(value_6))
print(type(value_7))
print(type(value_8))

# COMMAND ----------

# MAGIC %md
# MAGIC #  Complete Practical Program
# MAGIC

# COMMAND ----------

customer_record = {
    "customer_id": "101",
    "customer_name": " Anurag ",
    "order_amount": "2500.50",
    "is_active": "True",
    "discount": "20.50",
    "last_order_date": None
}

# COMMAND ----------

customer_id = int(customer_record["customer_id"])
customer_name = customer_record["customer_name"].strip()

order_amount = float(customer_record["order_amount"])

is_active = (
    customer_record["is_active"]
    .strip()
    .lower()
    == "true"
)

raw_discount = customer_record["discount"]

if raw_discount.strip().lower() in ["", "null", "none", "na", "n/a"]:
    discount = None
else:
    discount = float(raw_discount)

last_order_date = customer_record["last_order_date"]

cleaned_customer_record = {
    "customer_id": customer_id,
    "customer_name": customer_name,
    "order_amount": order_amount,
    "is_active": is_active,
    "discount": discount,
    "last_order_date": last_order_date
}

print("Raw Customer Record")
print("-------------------")
print(customer_record)

print()

print("Cleaned Customer Record")
print("-----------------------")
print(cleaned_customer_record)

print()

print("Cleaned Data Types")
print("------------------")
print("Customer ID:", type(cleaned_customer_record["customer_id"]))
print("Customer Name:", type(cleaned_customer_record["customer_name"]))
print("Order Amount:", type(cleaned_customer_record["order_amount"]))
print("Is Active:", type(cleaned_customer_record["is_active"]))
print("Discount:", type(cleaned_customer_record["discount"]))
print("Last Order Date:", type(cleaned_customer_record["last_order_date"]))


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
