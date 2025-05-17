{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "12151d86-52be-4562-8975-049e3776958a",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Class 3 Live Job Aggregator Project"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "cde2873c-a44b-457d-9011-b21d20543fa4",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## BRONZE LAYER _ Ingesting Data to Databricks"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "1a1e2834-a0c2-4c26-9c27-da2b3c07bcd6",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import requests,json\n",
    "from pyspark.sql import SparkSession\n",
    "\n",
    "#defining the Spark Session\n",
    "spark = SparkSession.builder.appName(\"JobETL_Bronze\").getOrCreate()\n",
    "\n",
    "#defining the API Key\n",
    "API_KEY = \"389429d5e4ce6553514c445f10f6ade53b4e34031a4c1c88f802ab97d22455be\"\n",
    "roles = [\"Data Engineer\", \"Python Developer\", \"ETL Developer\", \"Spark Engineer\", \"Data Analyst\"]\n",
    "location = \"India\"\n",
    "all_jobs = []\n",
    "\n",
    "#Running a Loop over all the Job Roles\n",
    "for role in roles:\n",
    "    params={\n",
    "        \"engine\":\"google_jobs\",\n",
    "        \"q\":role,\n",
    "        \"location\":location,\n",
    "        \"api_key\":API_KEY\n",
    "    }\n",
    "    res = requests.get(\"https://serpapi.com/search.json\", params=params)\n",
    "    print(\"✅Reading 🔴LIVE Data from Google Jobs API\")\n",
    "    jobs = res.json().get(\"jobs_results\", [])\n",
    "                        \n",
    "    for job in jobs:\n",
    "        job[\"search_role\"] = role\n",
    "    all_jobs.extend(jobs)\n",
    "\n",
    "bronze_df = spark.read.json(spark.sparkContext.parallelize([json.dumps(job) for job in all_jobs]))\n",
    "bronze_df.write.format(\"delta\").option(\"mergeSchema\", \"true\").mode(\"append\").save(\"/mnt/lakehouse/bronze/jobs_raw\")\n",
    "print(\"✅Data Written to Bronze Layer\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "f87a6519-0a72-43f6-b4f6-034a6aa69821",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# display(bronze_df)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "81036482-9594-49b8-949a-1444aa89355d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## SILVER LAYER ---> Cleaning and Structuring"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "fa1a7d81-15e2-45a0-bdd2-8d840b50f7d9",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import *\n",
    "\n",
    "spark=SparkSession.builder.appName(\"JobETL_Silver\").getOrCreate()\n",
    "\n",
    "#reading Bronze Layer\n",
    "print(\"✅Reading from Bronze Layer -------> Silver Layer\")\n",
    "df=spark.read.format(\"delta\").load(\"/mnt/lakehouse/bronze/jobs_raw\")\n",
    "\n",
    "#cleaning and Structuring\n",
    "silver_df=df.selectExpr(\n",
    "    \"title\",\n",
    "    \"company_name\",\n",
    "    \"location\",\n",
    "    \"description\",\n",
    "    \"job_id\",  \n",
    "    \"detected_extensions.posted_at as posted_at\",\n",
    "    \"search_role\"\n",
    ").dropna(subset=[\"title\", \"company_name\", \"location\"])\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "c483de22-fed3-4501-bbbf-d74102b65043",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# display(silver_df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "689592fa-ba26-41c6-9b81-7521661910d9",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "silver_df=silver_df.withColumn(\"company_name\",trim(upper(col(\"company_name\"))))\n",
    "silver_df=silver_df.dropna(subset=[\"posted_at\"])\n",
    "silver_df.write.format(\"delta\").option(\"mergeSchema\", \"true\").mode(\"append\").save(\"/mnt/lakehouse/silver/jobs\")\n",
    "print(\"✅ Data Written to Silver Layer!\")\n",
    "# display(silver_df)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "cfae1e7f-5e7e-440c-ae81-83e485224ac8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Golden LAYER ---> Generate KPI Tables"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "e3e5fd01-bed4-4059-b0f0-5694c606ef88",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "GOLDEN LAYER - : METHOD 1"
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import *\n",
    "\n",
    "spark=SparkSession.builder.appName(\"JobETL_Gold\").getOrCreate()\n",
    "\n",
    "print(\"✅Reading from Silver Layer -------> Golden Layer\")\n",
    "\n",
    "df=spark.read.format(\"delta\").load(\"/mnt/lakehouse/silver/jobs\")\n",
    "\n",
    "#writing the golden layer to a file \n",
    "df.write.format(\"delta\").option(\"mergeSchema\", \"true\").mode(\"append\").save(\"/mnt/lakehouse/gold/jobs\")\n",
    "#KPI 1 - Top Companies \n",
    "top_companies = df.groupBy(\"company_name\").count().orderBy(col(\"count\").desc())\n",
    "# display(top_companies)\n",
    "\n",
    "#KPI - Jobs by City \n",
    "df.groupBy('Location').agg(count('*').alias('job_count')).orderBy(col('job_count').desc())\n",
    "# display(top_companies)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "d96c7462-17fb-46f5-945b-43bb4434696d",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "GOLDEN LAYER - : METHOD 2"
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import *\n",
    "\n",
    "spark=SparkSession.builder.appName(\"JobETL_Gold\").getOrCreate()\n",
    "\n",
    "df=spark.read.format(\"delta\").load(\"/mnt/lakehouse/silver/jobs\")\n",
    "df.write.mode(\"append\").saveAsTable(\"JobAggregatorTable\")\n",
    "print(\"✅Data Successfully Written to Table ---> JobAggregatorTable\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "9bee2ffe-5f28-48e7-9706-36e6ce1dee70",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "print(\"😍Congratulations!!!!! Full ETL Job Aggregator Project is Completed and LIVE!!!!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "6bd12ed1-f191-4a45-bc9e-4471b23b4d82",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%sql \n",
    "describe history JobAggregatorTable"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "2c2b1764-1417-4215-af83-304ec3c5507b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "select * from JobAggregatorTable"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a66a0c74-e573-40d3-ba52-f566471f5401",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Data Analytics Part -: Visualising Business Scenrios"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "64ab4734-c57c-4bb0-9b62-895faefd594d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import matplotlib.pyplot as plt"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "bbf57399-82b8-4b1a-bde1-840ded3140d9",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "spark = SparkSession.builder.appName(\"VisualsFromGold\").getOrCreate()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "2aec8436-8705-4c93-b26a-15a3e408a44a",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Load gold layer: top companies\n",
    "df_companies = spark.read.format(\"delta\").load(\"/mnt/lakehouse/gold/jobs\")\n",
    "df_companies=df_companies.dropna(subset=[\"posted_at\"])\n",
    "display(df_companies)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "9e47d9e5-fa70-45e5-b57b-bc443219f944",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Convert to Pandas for plotting\n",
    "pdf = df_companies.orderBy(\"c\", ascending=True).limit(10).toPandas()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "b4a77cc9-c69f-403b-a5d6-5f2fdfc7e3ac",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "display(pdf)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "859acdff-5478-4bfc-b34e-29eac5ced8c3",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "#creating a plot\n",
    "plt.figure(figsize=(10,5))\n",
    "plt.barh(pdf['company_name'][::-1], pdf['title'][::-1], color='skyblue')\n",
    "plt.xlabel('Number of Jobs')\n",
    "plt.ylabel('Top Companies Hiring')\n",
    "plt.grid(True,axis='x',linestyle='--',alpha=0.5)\n",
    "plt.tight_layout()\n",
    "plt.show()\n",
    "\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "76317cec-24f2-4c36-9874-12c48ff7d75a",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Job Demand by City Plot"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "30a0836b-6e7f-4915-9cd1-cc266fd9b3ee",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "df_cities = spark.read.format(\"delta\").load(\"/mnt/lakehouse/gold/jobs\")\n",
    "df_cities=df_cities.dropna(subset=[\"posted_at\"])\n",
    "pdf_city = df_cities.orderBy(\"posted_at\", ascending=False).toPandas()\n",
    "\n",
    "# Plot\n",
    "plt.figure(figsize=(10, 6))\n",
    "plt.barh(pdf_city['location'][::-1], pdf_city['title'][::-1], color='lightgreen')\n",
    "plt.xlabel(\"Job Postings\")\n",
    "plt.title(\"Job Demand by City\")\n",
    "plt.grid(True, axis='x', linestyle='--', alpha=0.5)\n",
    "plt.tight_layout()\n",
    "plt.show()"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "2"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "mostRecentlyExecutedCommandWithImplicitDF": {
     "commandId": 7260343661037196,
     "dataframes": [
      "_sqldf"
     ]
    },
    "pythonIndentUnit": 4
   },
   "notebookName": "Class 3 - Job Aggregator Live Project",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
