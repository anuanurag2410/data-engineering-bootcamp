# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC Select "THis is DataX Bootcamp by Anurag" as Course

# COMMAND ----------

print("THis is DataX Bootcamp by Anurag")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # datax.dataengineering.bronze_orders

# COMMAND ----------

datax.dataengineering.bronze_orders

# COMMAND ----------

# DBTITLE 1,Cell 6
import pyspark.sql.functions as F

# --- Lazy Evaluation in Spark ---
# Spark transformations are LAZY: they build a plan but don't execute until an ACTION is called.

# 1. Define a DataFrame with transformations (no execution yet)
df = (spark.range(0, 10)
        .withColumnRenamed("id", "number")
        .withColumn("squared", F.col("number") * F.col("number"))
        .filter(F.col("number") > 3))

print("Transformations defined above -- nothing has run yet!")

# COMMAND ----------

# 2. Add another transformation (still lazy)
df_sorted = df.orderBy(F.col("squared").desc())

print("Added an ORDER BY -- still nothing executed. That's lazy evaluation!")

# 3. Call an ACTION -- NOW Spark builds the optimized plan and runs everything


# Key takeaway:
#   - Transformations (select, filter, join, withColumn, orderBy, ...) = LAZY
#   - Actions (show, count, collect, write, take, first) = EAGER (trigger execution)
# Spark can optimize the entire chain (e.g., push filters down) before running a single task.

# COMMAND ----------

print("Triggering an ACTION (show) -- Spark executes the full pipeline now:")
df_sorted.show()

# COMMAND ----------

import pyspark.sql.functions as F

# 1. Generate 10,000 numeric rows (0–9999) — lazy, nothing executes yet
df = spark.range(0, 10000)


# COMMAND ----------


# 2. Keep even IDs and derive an id_squared column — still lazy, just a new plan
df = (df.filter(F.col("id") % 2 == 0)
        .withColumn("id_squared", F.col("id") * F.col("id")))
print("Plan defined — no rows computed yet.")


# COMMAND ----------


# 3. Print the query plan so we can see operators (range → filter → projection)
#    explain() is a planning aid, not a data action.
print("\n--- EXPLAIN (logical + physical plan) ---")
df.explain()


# COMMAND ----------



# 4. Call an ACTION: count() forces Spark to execute and return a number.
row_count = df.count()
print(f"\ncount() returned: {row_count}")
print("Spark executed the plan — check the Spark UI for the job, its stages, and task counts.")
