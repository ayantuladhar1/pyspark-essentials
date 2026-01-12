# PySpark Case Study: Analyzing E-Commerce Transactions
## Scenario:
You have a dataset of e-commerce transactions, and you want to analyze customer purchase patterns, total spending, and the most frequently purchased products. The dataset contains the following columns:
* transaction_id: Unique ID for each transaction
* customer_id: Unique ID for each customer
* product_id: Unique ID for each product
* product_name: Name of the product
* category: Category of the product
* price: Price of the product
* quantity: Quantity purchased

The goal is to perform various data transformations to gain insights.

## Sample Data:
```csv
1, 101, 5001, 'Laptop', 'Electronics', 1000.0, 1
2, 102, 5002, 'Headphones', 'Electronics', 50.0, 2
3, 101, 5003, 'Book', 'Books', 20.0, 3
4, 103, 5004, 'Laptop', 'Electronics', 1000.0, 1
5, 102, 5005, 'Chair', 'Furniture', 150.0, 1
```
## Step 1: Setting Up PySpark
```python
from pyspark.sql import SparkSession
# Create SparkSession
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .master("local[*]") \
    .getOrCreate()

# Get SparkContext from SparkSession
sc = spark.sparkContext
```

## Step 2: Loading the Data into an RDD
```python
data = [
    "1,101,5001,Laptop,Electronics,1000.0,1",
    "2,102,5002,Headphones,Electronics,50.0,2",
    "3,101,5003,Book,Books,20.0,3",
    "4,103,5004,Laptop,Electronics,1000.0,1",
    "5,102,5005,Chair,Furniture,150.0,1"
]
transactions_rdd = sc.parallelize(data)
```

## Step 3: Transformations on Flat RDDs
* `map()` Transformation
  * Convert the CSV string into a tuple for better handling.
  * ```python transactions_tuple_rdd = transactions_rdd.map(lambda line: line.split(","))```
* `filter()` Transformation
  * Filter out transactions where the quantity is greater than 1. 
  * ```python high_quantity_rdd = transactions_tuple_rdd.filter(lambda x: int(x[6]) > 1)```
* `flatMap()` Transformation
  * Extract all products bought by customers to understand the diversity in purchases.
  * ```python products_flat_rdd = transactions_tuple_rdd.flatMap(lambda x: [x[3]])```

## Step 4: Working with Pair RDDs
* Creating a Pair RDD
  * Create a Pair RDD of (customer_id, (product_name, total_price)) for further analysis.
  * ```python pair_rdd = transactions_tuple_rdd.map(lambda x: (x[1], (x[3], float(x[5]) * int(x[6]))))```
* `reduceByKey()` Transformation
  * Find the total amount spent by each customer.
  * ```python customer_spending_rdd = pair_rdd.map(lambda x: (x[0], x[1][1])).reduceByKey(lambda x, y: x + y)```
* `groupByKey()` Transformation
  * Get a list of all products purchased by each customer.
  * ```python customer_products_rdd = pair_rdd.map(lambda x: (x[0], x[1][0])).groupByKey().mapValues(list)```

## Step 5: Join Operations on Pair RDDs
Join with product_category_rdd to get category information for each product purchased by customers.
```python
# Define the product_category_rdd for joining with transaction data
product_category_data = [
    ('Laptop', 'Electronics'),
    ('Headphones', 'Electronics'),
    ('Book', 'Books'),
    ('Chair', 'Furniture')
]
product_category_rdd = sc.parallelize(product_category_data)
customer_product_category_rdd = pair_rdd.map(lambda x: (x[1][0], (x[0], x[1][1]))).join(product_category_rdd)
```

## Step 6: Actions to Collect Results
* Collect Total Spending Per Customer
  * ```python total_spending = customer_spending_rdd.collect()```
* Collect Products Purchased Per Customer
  * ```python products_per_customer = customer_products_rdd.collect()```
* Collect Product-Category Join Results
  * ```python product_category_info = customer_product_category_rdd.collect()```

## Step 7: Save the Results
```python
# Save the results to a text file
customer_spending_rdd.saveAsTextFile("output/customer_spending")
customer_products_rdd.saveAsTextFile("output/customer_products")
customer_product_category_rdd.saveAsTextFile("output/customer_product_category")
```

# Summary of Transformations Used:

* `map`: Convert CSV strings to tuples.
* `filter`: Filter transactions based on a condition.
* `flatMap`: Extract all product names into a flat list.
* Pair RDD creation: Converting flat RDDs into key-value pairs.
* `reduceByKey`: Calculate total spending per customer.
* `groupByKey`: Collect a list of all products purchased by each customer.
* `join`: Combine product data with category information.
