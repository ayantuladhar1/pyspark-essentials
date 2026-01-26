# PySpark DataFrame & Hive–Based Telecommunication Data Processing Pipeline
## Solution Overview
To address large-scale telecommunication data processing requirements, this solution implements a PySpark DataFrame–based data cleansing and integration pipeline with Hive-backed persistence. The pipeline processes Call Detail Records (CDR) and Customer Usage Data stored in HDFS, ensuring data accuracy, consistency, and readiness for downstream analytics.
By leveraging Spark’s Catalyst Optimizer and Tungsten execution engine, the solution efficiently executes distributed transformations such as deduplication, null handling, standardization, and validation while maintaining high performance and fault tolerance.
The pipeline enables reliable analysis of:
* Call behavior and duration patterns
* Dropped and incomplete call identification
* Customer usage and billing insights
* Data completeness and consistency across datasets

## Technologies & Features Used
* PySpark DataFrames for schema-aware, optimized processing
* HDFS as the distributed data storage layer
* Apache Hive for managed, queryable data persistence
* Spark SQL for metadata management and validation queries
* In-memory distributed computation for performance
* Spark DAG & Lineage for fault-tolerant execution

# Optimization & Data Quality Strategy
|Business Requirement|	PySpark / Hive Feature Used|	Why It Helps|
|--------------------|-----------------------------|----------------|
|Process telecom data at scale|	DataFrames|	Columnar execution optimized by Catalyst|
|Remove duplicate records|	dropDuplicates()	|Prevents data inflation and skew|
|Handle missing values	|fillna()|	Ensures analytical completeness|
|Standardize categorical fields|	trim() + lower()|	Improves consistency across records|
|Enrich and persist clean data	|saveAsTable()|	Enables SQL-based access via Hive|
|Metadata management|	Hive Metastore|	Centralized schema and table tracking|
|Data validation|	Record count comparison|	Ensures ingestion accuracy|
|Reliability|	Spark DAG & Lineage|	Automatic recovery from failures|

## 1. Business Use Cases
## 1.1 Call Detail Records (CDR) Analysis
**Description**: Analyzing call detail records to identify patterns in call durations, dropped calls, and network issues. This involves handling inconsistencies, missing values, and removing duplicate records for accurate analysis.

## 1.2 Customer Usage Data Integration
**Description**: Integrating customer usage data from various sources to create a unified and consistent dataset. This includes handling missing values, removing duplicates, standardizing data formats, and ensuring data completeness.

## 2. Sample Data
## 2.1 Call Detail Records (CDR) Data (HDFS Source)
**Sample Records**:
|call_id| customer_id| call_start_time| call_end_time| call_duration| call_type| network_type| call_status|
|-------|------------|----------------|--------------|--------------|----------|-------------|------------|
|101| 1001| 2024-09-01 10:00:00| 2024-09-01 10:05:00| 300| outgoing| 4G| completed|
|102| 1002| 2024-09-01 11:00:00| 2024-09-01 11:10:00| 600| incoming| 4G| completed|
|103| 1002| 2024-09-01 11:00:00| 2024-09-01 11:10:00| 600| incoming| 4G| completed|
|104| 1003| 2024-09-01 12:00:00| NULL| NULL| outgoing| 3G| dropped|

## 2.2 Customer Usage Data (HDFS Source)
**Sample Records**:
|usage_id| customer_id| data_used_gb| voice_minutes_used| sms_used| billing_cycle_start| billing_cycle_end| total_charges|
|--------|------------|-------------|-------------------|---------|--------------------|------------------|--------------|
|201| 1001| 5.2| 150| 20| 2024-08-01| 2024-08-31| 50.00|
|202| 1002| 3.5| 200| 50| 2024-08-01| 2024-08-31| 40.00|
|203| 1002| 3.5| 200| 50| 2024-08-01| 2024-08-31| 40.00|
|204| 1003| NULL| NULL| 0| 2024-08-01| 2024-08-31| 30.00|

## 3. Data Cleaning and Transformation
## 3.1 PySpark Code for Data Cleaning
The following PySpark code demonstrates how to read data from HDFS, remove duplicates, handle null values, standardize data formats, and write the cleaned data to Hive tables.
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, lower

# Initialize Spark Session
spark = SparkSession.builder.appName("TelecomDataProcessing").enableHiveSupport().getOrCreate()

# Read Call Detail Records (CDR) Data from HDFS
cdr_df = spark.read.csv("hdfs://path/to/cdr_data.csv", header=True, inferSchema=True)

# Read Customer Usage Data from HDFS
usage_df = spark.read.csv("hdfs://path/to/customer_usage_data.csv", header=True, inferSchema=True)

# Data Cleaning: Remove Duplicates
cdr_df = cdr_df.dropDuplicates()
usage_df = usage_df.dropDuplicates()

# Handle Null Values: Replace NULLs with Default Values
cdr_df = cdr_df.fillna({"call_end_time": "unknown", "call_duration": 0, "call_status": "incomplete"})
usage_df = usage_df.fillna({"data_used_gb": 0.0, "voice_minutes_used": 0, "sms_used": 0})

# Standardize Data Formats: Trim Whitespaces and Convert to Lowercase
cdr_df = cdr_df.withColumn("call_type", trim(lower(col("call_type"))))
usage_df = usage_df.withColumn("total_charges", when(col("total_charges").isNull(), lit(0.0)).otherwise(col("total_charges")))

#Create Table in Hive
spark.sql("CREATE DATABASE IF NOT EXISTS telecom_db")

# Write Cleaned Data to Hive Tables
cdr_df.write.mode("overwrite").saveAsTable("telecom_db.cleaned_cdr_data")
usage_df.write.mode("overwrite").saveAsTable("telecom_db.cleaned_usage_data")
```
## Verify the Table Creation in Hive Directory

<img width="1052" height="413" alt="image" src="https://github.com/user-attachments/assets/f96ae3a4-7a2b-474b-991d-d0c7580f05c7" />

## Verify Databases and Tables in Hive

<img width="500" height="489" alt="image" src="https://github.com/user-attachments/assets/537254f9-ae38-4f67-ab6c-77666587b4ce" />

## Verify Table "cleaned_cdr_data"

<img width="1238" height="205" alt="image" src="https://github.com/user-attachments/assets/ba20f12d-eb58-4477-b70e-95ea86653f4d" />

## Verify Table "cleaned_usage_data"

<img width="933" height="181" alt="image" src="https://github.com/user-attachments/assets/ab4fa022-da23-41ac-a510-ac63758c2e3e" />

## 4. Data Completeness and Consistency Checks
After writing the cleaned data to Hive, it's essential to verify data completeness and consistency. This involves checking that all records from the source were correctly ingested into the target tables and that there are no discrepancies. The following PySpark code demonstrates how to perform these checks.
```python
# Check Record Counts Between Source and Target
source_cdr_count = cdr_df.count()
target_cdr_count = spark.sql("SELECT COUNT(*) FROM telecom_db.cleaned_cdr_data").collect()[0][0]
if source_cdr_count == target_cdr_count:
    print("CDR data is complete and consistent.")
else:
    print("Data inconsistency detected in CDR records.")
source_usage_count = usage_df.count()
target_usage_count = spark.sql("SELECT COUNT(*) FROM telecom_db.cleaned_usage_data").collect()[0][0]
if source_usage_count == target_usage_count:
    print("Customer usage data is complete and consistent.")
else:
    print("Data inconsistency detected in customer usage records.")
```

<img width="1232" height="156" alt="image" src="https://github.com/user-attachments/assets/a3cd5deb-339f-4066-9dcc-1d96bd578cc2" />

## 5. Conclusion
The above PySpark code demonstrates how to clean telecommunication data, handle duplicates and null values, standardize formats, and ensure data completeness and consistency when transforming data from HDFS to Hive. These practices are essential for maintaining accurate and reliable data in the telecommunication sector, where data quality is critical for decision-making and analysis.
