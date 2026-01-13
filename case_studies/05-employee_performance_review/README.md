# PySpark DataFrame Case Study: Employee Performance Review Analysis
## Scenario: You have a dataset containing employee performance reviews over time and want to analyze performance trends, average ratings, and identify high performers. The dataset contains the following columns:
* emp_id: Employee ID
* review_date: Date of the performance review
* department: Department name
* rating: Performance rating (1-5 scale)
* reviewer: Name of the person conducting the review

## Sample Data
| emp_id | review_date | department  | rating|  reviewer  |
|--------|-------------|-------------|-------|------------|
|   1    | 2024-01-10  | Engineering |   5   |   John     |
|   2    | 2024-01-11  | HR          |   4   |   Jane     |
|   3    | 2024-01-12  | Sales       |   3   |   Sam      |
|   4    | 2024-02-01  | Engineering |   5   |   John     |
|   1    | 2024-03-10  | Engineering |   4   |   Jane     |
|   2    | 2024-03-11  | HR          |  NULL |   Sam      |

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

## Filter
Find all reviews where the rating was 4 or higher.
```python
df_filtered = df.filter(df.rating >= 4)
df_filtered.show()
```

## Handle Null Values
Fill null values in the 'rating' column with the department average rating.
```python
window_spec = Window.partitionBy('department')
df_filled = df.withColumn('rating', coalesce(df.rating, avg('rating').over(window_spec)))
df_filled.show()
```

## Drop Duplicates
Remove duplicate reviews by 'emp_id' and 'review_date'.
```python
df_no_duplicates = df.dropDuplicates(['emp_id', 'review_date'])
df_no_duplicates.show()
```

## Select Specific Columns
Select 'emp_id', 'department', and 'rating' columns.
```python
df_selected = df.select('emp_id', 'department', 'rating')
df_selected.show()
```

## Grouping and Aggregating
Calculate the average rating per department.
```python
df_grouped = df.groupBy('department').agg({'rating': 'avg'})
df_grouped.show()
```

## Joining DataFrames
Join with another DataFrame 'df_employees' containing 'emp_id' and 'employee_name'.
```python
# Assuming df_employees is another DataFrame that contains employee details
df_joined = df.join(df_employees, on='emp_id', how='inner')
df_joined.show()
```

## Union of DataFrames
Union with another 'df_new_reviews' DataFrame containing additional reviews.
```python
df_union = df.union(df_new_reviews)
df_union.show()
```
## Temporary View and SQL
Create a temp view and find the average rating for each employee.
```python
df.createOrReplaceTempView('performance_reviews')
sql_result = spark.sql('SELECT emp_id, AVG(rating) as avg_rating FROM performance_reviews GROUP BY emp_id')
sql_result.show()
```
## Window Functions
Calculate the cumulative average rating for each employee over time.
```python
window_spec = Window.partitionBy('emp_id').orderBy('review_date')
df_with_cumulative_avg = df.withColumn('cumulative_avg', avg('rating').over(window_spec))
df_with_cumulative_avg.show()
```
