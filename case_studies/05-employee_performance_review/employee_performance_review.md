# PySpark DataFrame & SQL–Based Employee Performance Review Analytics Pipeline
## Solution Overview
To analyze employee performance reviews over time, we implement a PySpark DataFrame–based analytics pipeline augmented with Spark SQL and Window Functions. This solution enables scalable, structured analysis of performance trends, departmental averages, and individual employee growth patterns.
By leveraging Spark’s Catalyst Optimizer and Tungsten execution engine, the pipeline efficiently processes review data in parallel, automatically optimizing execution plans, memory usage, and aggregations. The use of window functions allows time-based analysis without sacrificing performance or scalability.
The solution enables comprehensive analysis of:
* Employee performance trends over time
* Department-level average ratings
* Identification of high-performing employees
* Review completeness and data quality
* Incremental ingestion of new performance reviews

## Technologies & Features Used
* PySpark DataFrames for schema-aware, optimized processing
* Spark SQL for declarative, SQL-based analytics
* Window Functions for time-series and cumulative analysis
* Built-in transformations for filtering, aggregation, joins, and unions
* In-memory distributed computation for high performance
* Fault-tolerant execution via Spark DAG and lineage

## Optimization Strategy
|Business Requirement|	PySpark Feature Used|	Why It Helps|
|--------------------|----------------------|-------------|
|Process review data at scale|	DataFrames|	Columnar execution optimized by Catalyst|
|Enforce schema consistency|	Schema-based DataFrames|	Ensures data quality and correctness|
|Identify high performers|	filter()|	Predicate pushdown reduces data scanned|
|Handle missing ratings|	coalesce() + window avg|	Preserves departmental context|
|Remove duplicate reviews|	dropDuplicates()|	Prevents skewed performance metrics|
|Department-level insights|	groupBy() + avg()|	Efficient distributed aggregation|
|Enrich review data|	join()|	Optimized hash and sort-merge joins|
|Append new review data|	union()|	Supports incremental processing|
|Employee-level analytics|	Spark SQL|	Declarative and optimized querying|
|Performance trends over time|	Window Functions|	Time-aware analytics without data reshuffling|
|Reliability at scale|	Spark DAG & Lineage|	Automatic fault recovery|

## Code to Create DataFrame
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, coalesce
from pyspark.sql.window import Window
# Initialize Spark session
spark = SparkSession.builder.appName("Employee Performance Review Analysis").getOrCreate()
# Sample data
data = [
    (1, '2024-01-10', 'Engineering', 5, 'John'),
    (2, '2024-01-11', 'HR', 4, 'Jane'),
    (3, '2024-01-12', 'Sales', 3, 'Sam'),
    (4, '2024-02-01', 'Engineering', 5, 'John'),
    (1, '2024-03-10', 'Engineering', 4, 'Jane'),
    (2, '2024-03-11', 'HR', None, 'Sam')
]
# Create DataFrame
columns = ["emp_id", "review_date", "department", "rating", "reviewer"]
df = spark.createDataFrame(data, schema=columns)
df.show()
```

<img width="1195" height="302" alt="image" src="https://github.com/user-attachments/assets/fda43efb-a424-480f-a093-976f5451f9f7" />

## Step 1: Filter
```python
#Find all reviews where the rating was 4 or higher.
df_filtered = df.filter(df.rating >= 4)
df_filtered.show()
```

<img width="1185" height="243" alt="image" src="https://github.com/user-attachments/assets/fbc101fb-d53b-48e5-b053-ca4cdf627b92" />

## Step 2: Handle Null Values
```python
#Fill null values in the 'rating' column with the department average rating.
window_spec = Window.partitionBy('department')
df_filled = df.withColumn('rating', coalesce(df.rating, avg('rating').over(window_spec)))
df_filled.show()
```

<img width="1185" height="301" alt="image" src="https://github.com/user-attachments/assets/5bc05245-a3e4-4714-b4c7-0bae7195bd45" />

## Step 3: Drop Duplicates
```python
#Remove duplicate reviews by 'emp_id' and 'review_date'.
df_no_duplicates = df.dropDuplicates(['emp_id', 'review_date'])
df_no_duplicates.show()
```

<img width="512" height="253" alt="image" src="https://github.com/user-attachments/assets/04c134bd-95c3-4fb0-ad3c-44575615507e" />
<img width="1190" height="303" alt="image" src="https://github.com/user-attachments/assets/3456687a-cb6d-4e38-a4f8-081d22b790b3" />

## Step 4: Select Specific Columns
```python
#Select 'emp_id', 'department', and 'rating' columns.
df_selected = df.select('emp_id', 'department', 'rating')
df_selected.show()
```

<img width="1188" height="322" alt="image" src="https://github.com/user-attachments/assets/34dd4bb1-3a88-428e-811d-a753faac755d" />


## Step 5: Grouping and Aggregating
```python
#Calculate the average rating per department.
df_grouped = df.groupBy('department').agg({'rating': 'avg'})
df_grouped.show()
```

<img width="1183" height="237" alt="image" src="https://github.com/user-attachments/assets/55c6838e-d17d-42f0-b0f8-76f36035cdce" />

## Step 6: Joining DataFrames
```python
#Join with another DataFrame 'df_employees' containing 'emp_id' and 'employee_name'.
# Assuming df_employees is another DataFrame that contains employee details
df_joined = df.join(df_employees, on='emp_id', how='inner')
df_joined.show()
```

<img width="819" height="397" alt="image" src="https://github.com/user-attachments/assets/89d96e34-2fc8-4573-9895-964d8bd8fafa" />
<img width="1193" height="422" alt="image" src="https://github.com/user-attachments/assets/da86a18d-9222-4d23-a68d-44fec3def738" />

## Step 7: Union of DataFrames
```python
#Union with another 'df_new_reviews' DataFrame containing additional reviews.
df_union = df.union(df_new_reviews)
df_union.show()
```

<img width="807" height="337" alt="image" src="https://github.com/user-attachments/assets/2f58c2d5-6537-4b43-93d8-6a76ffb10658" />
<img width="1184" height="394" alt="image" src="https://github.com/user-attachments/assets/2020aea1-a4c9-41ea-b251-16476d7820ea" />

## Step 8: Temporary View and SQL
```python
#Create a temp view and find the average rating for each employee.
df.createOrReplaceTempView('performance_reviews')
sql_result = spark.sql('SELECT emp_id, AVG(rating) as avg_rating FROM performance_reviews GROUP BY emp_id')
sql_result.show()
```

<img width="1188" height="263" alt="image" src="https://github.com/user-attachments/assets/7e8f2959-3556-41c9-afe9-3781d75334b1" />

## Step 9: Window Functions
```python
#Calculate the cumulative average rating for each employee over time.
window_spec = Window.partitionBy('emp_id').orderBy('review_date')
df_with_cumulative_avg = df.withColumn('cumulative_avg', avg('rating').over(window_spec))
df_with_cumulative_avg.show()3
```
<img width="1186" height="319" alt="image" src="https://github.com/user-attachments/assets/e8ed00f0-c54c-449d-acb3-2e97b69ed4e2" />
