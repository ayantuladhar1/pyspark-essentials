# PySpark DataFrame & SQL–Based Analytics Pipeline
## Solution Overview
To address large-scale e-commerce transaction analytics, we implement a PySpark DataFrame–based solution combined with Spark SQL. This approach provides a high-level, optimized, and declarative analytics layer that is easier to maintain and more performant than low-level RDD operations.
By leveraging Spark’s Catalyst Optimizer and Tungsten execution engine, the pipeline efficiently processes transaction data in parallel while automatically optimizing query plans, memory usage, and execution strategies.
The solution enables fast, scalable analysis of:
* Customer spending behavior
* Product purchasing trends
* Category-wise performance
* Repeat and bulk buying patterns

## Technologies & Features Used
* PySpark DataFrames for structured, schema-aware processing
* Spark SQL for expressive, SQL-based analytics
* Built-in transformations for filtering, aggregation, and joins
* In-memory distributed computation for high performance
* Fault-tolerant execution through Spark’s DAG and lineage

## Optimization Strategy
|Business Requirement|	PySpark Feature Used|	Why It Helps|
|--------------------|----------------------|-------------|
|Process large-scale transactional data|	DataFrames	|Columnar, optimized execution with Catalyst|
|Clean & structure raw data|	Schema-based DataFrames|	Enforces data types and consistency|
|Filter bulk or repeat purchases|	filter()|	Predicate pushdown reduces data scanned|
|Handle missing or bad data|	na.fill()|	Improves data quality for analytics|
|Remove duplicate transactions|	dropDuplicates()|	Ensures accurate aggregation|
|Analyze customer spending|	groupBy() + agg()|	Optimized distributed aggregation|
|Combine reference data|	join()|	Efficient hash and sort-merge joins|
|Combine new and historical data|	union()|	Enables incremental data processing|
|Ad-hoc analytics & reporting|	Spark SQL	|Familiar SQL interface with optimizer|
|Reliability at scale|	Spark DAG & Lineage|	Automatic fault recovery|

## Step 1: Loading the Data into a DataFrame
```python
from pyspark.sql import SparkSession
# Initialize SparkSession
spark = SparkSession.builder.appName("E-Commerce Analysis").getOrCreate()
# Sample data
data = [
    (1, 101, 5001, 'Laptop', 'Electronics', 1000.0, 1),
    (2, 102, 5002, 'Headphones', 'Electronics', 50.0, 2),
    (3, 101, 5003, 'Book', 'Books', 20.0, 3),
    (4, 103, 5004, 'Laptop', 'Electronics', 1000.0, 1),
    (5, 102, 5005, 'Chair', 'Furniture', 150.0, 1)
]
columns = ["transaction_id", "customer_id", "product_id", "product_name", "category", "price", "quantity"]
# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()
```

<img width="867" height="384" alt="image" src="https://github.com/user-attachments/assets/2790f3b6-8fc7-49db-940c-6c0876185de0" />

## Step 2: Using Filter Transformation
```python
# Filter transactions where quantity is greater than 1
df_filtered = df.filter(df.quantity > 1)
df_filtered.show()
```

<img width="856" height="200" alt="image" src="https://github.com/user-attachments/assets/d6d6d870-0672-47ea-bc06-33077a8f9cf0" />

## Step 3: Handling Null Values
```python
# Filling null values in price column with the average price
average_price = df.selectExpr("avg(price)").collect()[0][0]
df_filled = df.na.fill({"price": average_price})
df_filled.show()
```

<img width="858" height="266" alt="image" src="https://github.com/user-attachments/assets/2f7b8b75-e821-4761-80f0-034ea8706e56" />

## Step 4: Dropping Duplicates
```python
# Drop duplicate rows based on customer_id and product_id
df_no_duplicates = df.dropDuplicates(["customer_id", "product_id"])
df_no_duplicates.show()
```

<img width="856" height="252" alt="image" src="https://github.com/user-attachments/assets/ff3c4ae0-b0e2-42e9-ad97-9d8c021cf6a1" />

## Step 5: Selecting Specific Columns
```python
# Select specific columns
df_selected = df.select("customer_id", "product_name", "price")
df_selected.show()
```

<img width="860" height="262" alt="image" src="https://github.com/user-attachments/assets/16e6b0cb-fbab-46aa-84d3-0394c028905f" />

## Step 6: Grouping and Aggregating Data
```python
# Calculate the total spending per customer
df_grouped = df.groupBy("customer_id").agg({"price": "sum"})
df_grouped.show()
```

<img width="857" height="214" alt="image" src="https://github.com/user-attachments/assets/e6a854a4-a425-4546-accf-ed430626ce78" />

## Step 7: Joining DataFrames
```python
# Assume we have another DataFrame with customer details
customer_data = [
    (101, "John Doe", "john@example.com"),
    (102, "Jane Smith", "jane@example.com"),
    (103, "Alice Johnson", "alice@example.com")
]
customer_columns = ["customer_id", "customer_name", "email"]
df_customers = spark.createDataFrame(customer_data, customer_columns)

# Join on customer_id
df_joined = df.join(df_customers, on="customer_id", how="inner")
df_joined.show()
```

<img width="1054" height="270" alt="image" src="https://github.com/user-attachments/assets/2f22b898-019f-4c89-958f-a7352a718886" />

## Step 8: Union of Two DataFrames
```python
# Create another DataFrame with similar schema
new_data = [
    (6, 104, 5006, 'Table', 'Furniture', 200.0, 1)
]
df_new = spark.createDataFrame(new_data, columns)

# Union the DataFrames
df_union = df.union(df_new)
df_union.show()
```

<img width="858" height="287" alt="image" src="https://github.com/user-attachments/assets/85ad5eb6-9af2-4124-a8d2-f9e38ca282a5" />

## Step 9: Creating Temporary Views and Using SQL
```python
# Create a temporary view
df.createOrReplaceTempView("transactions")

# Run SQL query
sql_result = spark.sql("SELECT customer_id, SUM(price * quantity) as total_spent FROM transactions GROUP BY customer_id")
sql_result.show()
```

<img width="863" height="217" alt="image" src="https://github.com/user-attachments/assets/12c9e65e-e670-4479-884c-748b279122e2" />

## Why DataFrames + SQL Over Raw RDDs
* Less code, more readability
* Automatic query optimization
* Better performance through Catalyst
* Native SQL support for analysts
* Easier debugging and maintenance
This makes the solution ideal for production-grade analytics pipelines and enterprise data platforms.

## Final Outcome
By combining PySpark DataFrames with Spark SQL, the organization gains a scalable, fault-tolerant, and high-performance analytics solution that transforms raw transaction data into actionable business intelligence—without sacrificing reliability or speed as data volume grows.
