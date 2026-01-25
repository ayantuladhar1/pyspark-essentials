# PySpark DataFrame & SQL–Based Sales Transactions Analytics Pipeline
## Solution Overview
To analyze large-scale sales transaction data, we implement a PySpark DataFrame–based analytics pipeline combined with Spark SQL. This approach provides a scalable, schema-aware, and optimized processing layer capable of handling high transaction volumes while delivering accurate business insights.
By leveraging Spark’s Catalyst Optimizer and Tungsten execution engine, the pipeline automatically optimizes query plans, memory usage, and execution strategies. This enables efficient parallel processing of transactional data without relying on low-level RDD operations.
The solution enables fast and scalable analysis of:
* Total sales and revenue generation
* Popular and high-performing products
* Customer purchasing behavior
* Incremental ingestion of new transactions

## Technologies & Features Used
* PySpark DataFrames for structured, optimized data processing
* Spark SQL for declarative, SQL-based analytics
* Built-in transformations for filtering, aggregation, joins, and unions
* In-memory distributed computation for high-performance execution
* Fault-tolerant execution via Spark DAG and lineage

## Optimization Strategy
|Business Requirement|	PySpark Feature Used|	Why It Helps|
|--------------------|----------------------|-------------|
|Process transactional data at scale|	DataFrames|	Columnar execution optimized by Catalyst|
|Enforce data consistency|	Schema-based DataFrames|	Ensures correct data types|
|Identify high-value transactions|	filter()|	Predicate pushdown minimizes data scan|
|Handle missing quantities|	na.fill()|	Prevents nulls from affecting calculations|
|Remove duplicate transactions|	dropDuplicates()|	Ensures accurate sales metrics|
|Calculate product-level sales|	groupBy() + sum()|	Optimized distributed aggregation|
|Enrich transactions with metadata|	join()|	Efficient hash/sort-merge joins|
|Append new transaction data|	union()|	Supports incremental data processing|
|Ad-hoc customer analytics|	Spark SQL|	SQL interface with optimizer support|
|Reliability and recovery|	Spark DAG & Lineage|	Automatic fault tolerance|

## Code to Create DataFrame
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
# Initialize Spark session
spark = SparkSession.builder.appName("Sales Transactions Analysis").getOrCreate()
# Sample data
data = [
    (1001, 2001, 3001, 5, 20.0, '2024-09-01'),
    (1002, 2002, 3002, 2, 50.0, '2024-09-01'),
    (1003, 2003, 3003, 1, 120.0, '2024-09-02'),
    (1004, 2001, 3002, 3, 40.0, '2024-09-03'),
    (1005, 2004, 3001, 10, 15.0, '2024-09-03'),
    (1006, 2005, 3004, None, 30.0, '2024-09-03')
]
# Create DataFrame
columns = ["transaction_id", "customer_id", "product_id", "quantity", "price", "date"]
df = spark.createDataFrame(data, schema=columns)
df.show()
```

<img width="1249" height="296" alt="image" src="https://github.com/user-attachments/assets/0df24faf-6c1b-44bf-877b-a8f37ad8723d" />

## Step1: Filter
```python
#Find transactions where the total amount (quantity * price) is greater than $100.
df_filtered = df.filter((df.quantity * df.price) > 100)
df_filtered.show()
```

<img width="1250" height="232" alt="image" src="https://github.com/user-attachments/assets/9bf62141-941c-4f05-aec6-80dce57bc5f0" />

## Step2: Handle Null Values
```python
#Replace null values in the 'quantity' column with 0.
df_filled = df.na.fill({'quantity': 0})
df_filled.show()
```

<img width="1264" height="299" alt="image" src="https://github.com/user-attachments/assets/64dd9b08-6f14-45b8-b938-a8a5e4b7c875" />

## Step3: Drop Duplicates
```python
#Remove duplicate transactions by 'transaction_id'.
df_no_duplicates = df.dropDuplicates(['transaction_id'])
df_no_duplicates.show()
```

<img width="526" height="265" alt="image" src="https://github.com/user-attachments/assets/e835a217-a80a-4275-b5f5-e0755e482c40" />
<img width="1261" height="304" alt="image" src="https://github.com/user-attachments/assets/f1629738-4ced-442c-9207-967b7a393292" />

## Step4: Select Specific Columns
```python
#Select 'customer_id', 'product_id', and 'quantity'.
df_selected = df.select('customer_id', 'product_id', 'quantity')
df_selected.show()
```

<img width="1258" height="323" alt="image" src="https://github.com/user-attachments/assets/f5daca15-eec4-4ba6-abaf-ddd93c2d79b4" />

## Step5: Grouping and Aggregating
```python
#Calculate total sales per product.
df_grouped = df.groupBy('product_id').agg({'quantity': 'sum'})
df_grouped.show()
```

<img width="1259" height="257" alt="image" src="https://github.com/user-attachments/assets/5751233e-84fc-4de6-852e-37fa2910e01d" />

## Step6: Joining DataFrames
```python
#Join with a 'product_details' DataFrame containing 'product_id' and 'product_name'.
#Assuming df_products is another DataFrame that contains product details
df_joined = df.join(df_products, on='product_id', how='inner')
df_joined.show()
```

<img width="839" height="395" alt="image" src="https://github.com/user-attachments/assets/9f044e9f-641f-4248-8efc-376c16216754" />
<img width="1263" height="416" alt="image" src="https://github.com/user-attachments/assets/ce4dc972-c6e5-4e65-b5d5-0942af55cd4b" />

## Step7: Union of DataFrames
```python
#Union this DataFrame with another 'df_new_transactions'.
df_union = df.union(df_new_transactions)
df_union.show()
```

<img width="970" height="356" alt="image" src="https://github.com/user-attachments/assets/3c556be3-23c7-4354-b49a-7f3ec90405e5" />
<img width="1261" height="405" alt="image" src="https://github.com/user-attachments/assets/f40aa907-dce4-4f1e-94b8-0da781b6a384" />

## Step8: Temporary View and SQL
```python
#Create a temp view and calculate the total sales per customer.
df.createOrReplaceTempView('transactions')
sql_result = spark.sql('SELECT customer_id, SUM(quantity * price) as total_spent FROM transactions GROUP BY customer_id')
sql_result.show()
```
<img width="1249" height="285" alt="image" src="https://github.com/user-attachments/assets/f057b319-a9bd-4b80-898e-d173df9200be" />

## Why DataFrames + SQL Over Raw RDDs
* Reduced code complexity and higher readability
* Automatic query optimization via Catalyst
* Better performance through Tungsten execution
* Native SQL support for analytics teams
* Easier debugging, testing, and maintenance

This makes the solution ideal for production-grade analytics pipelines and enterprise-scale data platforms.

## Final Outcome
By combining PySpark DataFrames with Spark SQL, the organization gains a scalable, fault-tolerant, and high-performance sales analytics solution that transforms raw transaction data into actionable business intelligence—while maintaining performance and reliability as data volume grows.
