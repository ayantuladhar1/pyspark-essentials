# PySpark DataFrame Case Study: Sales Transactions Analysis
## Scenario: You have a dataset of sales transactions, and the goal is to analyze total sales, popular products, and customer purchasing patterns. The dataset contains the following columns:
* transaction_id: Transaction ID
* customer_id: Customer ID
* product_id: Product ID
* quantity: Quantity purchased
* price: Price per unit
* date: Transaction date

## Sample Data
| transaction_id| customer_id| product_id| quantity|  price|     date     |
|---------------|------------|-----------|---------|-------|--------------|
|      1001     |    2001    |    3001   |    5    |  20.0 |   2024-09-01 |
|      1002     |    2002    |    3002   |    2    |  50.0 |   2024-09-01 |
|      1003     |    2003    |    3003   |    1    |  120.0|   2024-09-02 |
|      1004     |    2001    |    3002   |    3    |  40.0 |   2024-09-03 |
|      1005     |    2004    |    3001   |    10   |  15.0 |   2024-09-03 |
|      1006     |    2005    |    3004   |    NULL |  30.0 |   2024-09-03 |

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

1. Filter
Find transactions where the total amount (quantity * price) is greater than $100.
```python
df_filtered = df.filter((df.quantity * df.price) > 100)
df_filtered.show()
```

## Handle Null Values
Replace null values in the 'quantity' column with 0.
```python
df_filled = df.na.fill({'quantity': 0})
df_filled.show()
```

## Drop Duplicates
Remove duplicate transactions by 'transaction_id'.
```python
df_no_duplicates = df.dropDuplicates(['transaction_id'])
df_no_duplicates.show()
```

## Select Specific Columns
Select 'customer_id', 'product_id', and 'quantity'.
```python
df_selected = df.select('customer_id', 'product_id', 'quantity')
df_selected.show()
```

## Grouping and Aggregating
Calculate total sales per product.
```python
df_grouped = df.groupBy('product_id').agg({'quantity': 'sum'})
df_grouped.show()
```

## Joining DataFrames
Join with a 'product_details' DataFrame containing 'product_id' and 'product_name'.
```python
# Assuming df_products is another DataFrame that contains product details
df_joined = df.join(df_products, on='product_id', how='inner')
df_joined.show()
```

## Union of DataFrames
Union this DataFrame with another 'df_new_transactions'.
```python
df_union = df.union(df_new_transactions)
df_union.show()
```

## Temporary View and SQL
Create a temp view and calculate the total sales per customer.
```python
df.createOrReplaceTempView('transactions')
sql_result = spark.sql('SELECT customer_id, SUM(quantity * price) as total_spent FROM transactions GROUP BY customer_id')
sql_result.show()
```
