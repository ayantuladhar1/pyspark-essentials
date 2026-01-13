# Business Problem
An e-commerce company processes millions of daily transactions across multiple products, categories, and customers.
As transaction volume grows, it becomes increasingly difficult to:
* Track customer spending patterns
* Identify top-selling products
* Understand category-wise performance
* Detect repeat and bulk buyers
Without an optimized data processing layer, running analytics on raw transaction data becomes slow, expensive, and unreliable, impacting marketing, inventory planning, and revenue forecasting.

# Solution Overview
To solve this, we use PySpark on a distributed cluster (Hadoop / Spark) to build a scalable, fault-tolerant analytics pipeline that processes transaction data in parallel.
### We leverage:
* RDDs for distributed data processing
* Transformations for filtering, mapping, and grouping
* Pair RDDs for aggregation and joins
## This allows us to efficiently compute:
* Customer-wise total spending
* Products purchased per customer
* Category-wise product insights

# Optimization Strategy

|Business Requirement|	PySpark Feature Used|	Why It Helps|
|--------------------|----------------------|-------------|
|Process millions of transactions|	RDDs|	Distributed, parallel processing across nodes
|Clean & structure raw CSV data|	map()|	Convert raw strings into structured tuples
|Filter bulk purchases|	filter()|	Avoid unnecessary data processing
|Extract products|	flatMap()|	Efficiently expand nested data
|Calculate customer spending|	reduceByKey()|	Distributed aggregation without shuffling all data
|List all products per customer|	groupByKey()|	Groups purchases per customer
|Attach category info|	join()|	Combine transaction data with reference data
|Fault tolerance|	RDD Lineage|	Automatic recovery from failures

# Step 1: Setup PySpark, Load the Data
```python
if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("01-case-study-rdd") \
        .getOrCreate()

    sc = spark.sparkContext
    data = [
        "1,101,5001,Laptop,Electronics,1000.0,1",
        "2,102,5002,Headphones,Electronics,50.0,2",
        "3,101,5003,Book,Books,20.0,3",
        "4,103,5004,Laptop,Electronics,1000.0,1",
        "5,102,5005,Chair,Furniture,150.0,1"
    ]
    transactions_rdd = sc.parallelize(data)
```
# Step 2: Transform the following Pyspark Features on RDD

## map() Transformation
Convert the CSV string into a tuple for better handling.
```python
transactions_tuple_rdd = transactions_rdd.map(lambda line: line.split(","))
    print(transactions_tuple_rdd.collect())
```
<img width="2043" height="139" alt="image" src="https://github.com/user-attachments/assets/2d9f16d5-73dc-4e1d-b96d-7a2126fa4710" />

## filter() Transformation
Filter out transactions where the quantity is greater than 1.
```python
high_quantity_rdd = transactions_tuple_rdd.filter(lambda x: int(x[6]) > 1)
    print(high_quantity_rdd.collect())
```
<img width="1171" height="120" alt="image" src="https://github.com/user-attachments/assets/6200bb08-5019-419b-83fc-f3e08119cedb" />

## flatMap() Transformation
Extract all products bought by customers to understand the diversity in purchases.
```python
products_flat_rdd = transactions_tuple_rdd.flatMap(lambda x: [x[3]])
    print(products_flat_rdd.collect())
```
<img width="1015" height="117" alt="image" src="https://github.com/user-attachments/assets/8573d49e-0748-41d0-88cb-b5a5f12f1f88" />

# Step 3: Working with Pair RDDs
## Create a Pair RDD of (customer_id, (product_name, total_price)) for further analysis.
```python
pair_rdd = transactions_tuple_rdd.map(lambda x: (x[1], (x[3], float(x[5]) * int(x[6]))))
    print(pair_rdd.collect())
```
<img width="1431" height="129" alt="image" src="https://github.com/user-attachments/assets/6807cb9b-a26e-4da3-8434-4bd1a4fa167c" />

## reduceByKey() Transformation
Find the total amount spent by each customer.
```python
customer_spending_rdd = pair_rdd.map(lambda x: (x[0], x[1][1])).reduceByKey(lambda x, y: x + y)
    print(customer_spending_rdd.collect())
```
<img width="1020" height="121" alt="image" src="https://github.com/user-attachments/assets/7554b276-0a93-4b16-9a69-c0e7d23305e8" />

## groupByKey() Transformation
Get a list of all products purchased by each customer.
```python
customer_products_rdd = pair_rdd.map(lambda x: (x[0], x[1][0])).groupByKey().mapValues(list)
    print( customer_products_rdd.collect())
```
<img width="1038" height="140" alt="image" src="https://github.com/user-attachments/assets/bf4619e8-5d44-4728-aa31-d05cb8a41d1e" />

# Step 5: Join Operations on Pair RDDs
## Join with product_category_rdd to get category information for each product purchased by customers.
```python
product_category_data = [
        ('Laptop', 'Electronics'),
        ('Headphones', 'Electronics'),
        ('Book', 'Books'),
        ('Chair', 'Furniture')
    ]
    product_category_rdd = sc.parallelize(product_category_data)
    customer_product_category_rdd = pair_rdd.map(lambda x: (x[1][0], (x[0], x[1][1]))).join(product_category_rdd)
    print(customer_product_category_rdd.collect())
```
<img width="2046" height="136" alt="image" src="https://github.com/user-attachments/assets/b0b161a3-f1ed-4576-acee-29da01ff0f4c" />

# Step 6: Save the Results
The following saves the results into text file
```python
customer_spending_rdd.saveAsTextFile("file:///home/takeo/pycharmprojects/PythonProject/rdd-output/customer_spending")
```
<img width="1054" height="164" alt="image" src="https://github.com/user-attachments/assets/87d74c14-f089-4d62-ab8e-3af1681a504d" />

```python
customer_products_rdd.saveAsTextFile("file:///home/takeo/pycharmprojects/PythonProject/rdd-output/customer_products")
```
<img width="1048" height="161" alt="image" src="https://github.com/user-attachments/assets/2c2d45eb-b896-48b7-9a2d-aed16419f926" />

```python
customer_product_category_rdd.saveAsTextFile("file:///home/takeo/pycharmprojects/PythonProject/rdd-output/customer_product_category")
```
<img width="1129" height="227" alt="image" src="https://github.com/user-attachments/assets/7e784956-7f52-49df-bb55-801d0acef1c7" />
