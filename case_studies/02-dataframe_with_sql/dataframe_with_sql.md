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

Step 1: Loading the Data into a DataFrame

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




Step 2: Using Filter Transformation

# Filter transactions where quantity is greater than 1
df_filtered = df.filter(df.quantity > 1)
df_filtered.show()




Step 3: Handling Null Values

# Filling null values in price column with the average price
average_price = df.selectExpr("avg(price)").collect()[0][0]
df_filled = df.na.fill({"price": average_price})
df_filled.show()



Step 4: Dropping Duplicates

# Drop duplicate rows based on customer_id and product_id
df_no_duplicates = df.dropDuplicates(["customer_id", "product_id"])
df_no_duplicates.show()



Step 5: Selecting Specific Columns

# Select specific columns
df_selected = df.select("customer_id", "product_name", "price")
df_selected.show()



Step 6: Grouping and Aggregating Data

# Calculate the total spending per customer
df_grouped = df.groupBy("customer_id").agg({"price": "sum"})
df_grouped.show()



Step 7: Joining DataFrames

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



Step 8: Union of Two DataFrames

# Create another DataFrame with similar schema
new_data = [
    (6, 104, 5006, 'Table', 'Furniture', 200.0, 1)
]
df_new = spark.createDataFrame(new_data, columns)

# Union the DataFrames
df_union = df.union(df_new)
df_union.show()


Step 9: Creating Temporary Views and Using SQL

# Create a temporary view
df.createOrReplaceTempView("transactions")

# Run SQL query
sql_result = spark.sql("SELECT customer_id, SUM(price * quantity) as total_spent FROM transactions GROUP BY customer_id")
sql_result.show()

<img width="1069" height="6028" alt="image" src="https://github.com/user-attachments/assets/21a8346d-ebc6-4d35-b715-8a564bdf24d5" />
