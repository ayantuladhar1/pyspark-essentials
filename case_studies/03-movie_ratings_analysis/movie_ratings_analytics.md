# PySpark DataFrame Case Study: Movie Ratings Analysis — Solution Statement
## Solution Overview
To analyze large-scale movie rating data submitted by users, we implement a PySpark DataFrame–based analytics pipeline combined with Spark SQL. This approach provides a structured, scalable, and highly optimized analytics layer capable of efficiently processing user interactions and deriving meaningful insights from rating behavior.
By leveraging Spark’s Catalyst Optimizer and Tungsten execution engine, the pipeline automatically optimizes query execution, memory management, and join strategies, ensuring high performance even as data volume grows.
The solution enables efficient analysis of:
* User rating behavior and preferences
* Average ratings per movie
* Identification of top-rated and popular movies
* Data enrichment through joins
* Incremental data ingestion through unions

## Technologies & Features Used
* PySpark DataFrames for schema-aware, optimized data processing
* Spark SQL for expressive, SQL-based analytical queries
* Built-in transformations for filtering, aggregation, joins, and unions
* In-memory distributed computation for high-performance analytics
* Fault-tolerant execution using Spark’s DAG and lineage model

## Optimization Strategy

|Business Requirement|	PySpark Feature Used|	Why It Helps|
|--------------------|----------------------|-------------|
|Process user rating data at scale|	DataFrames|	Columnar execution optimized by Catalyst|
|Clean and structure raw ratings|	Schema-based DataFrames|	Enforces consistency and data types|
|Filter high-quality ratings	|filter()|	Predicate pushdown reduces data scanned|
|Handle missing ratings	|na.fill()|	Prevents nulls from impacting aggregations|
|Remove duplicate user ratings	|dropDuplicates()|	Ensures analytical accuracy|
|Compute movie popularity	|groupBy() + avg()|	Efficient distributed aggregation|
|Enrich ratings with metadata	|join()|	Optimized hash and sort-merge joins|
|Append new rating data	|union()|	Enables incremental data processing|
|Ad-hoc analysis & reporting	|Spark SQL|	Declarative SQL with optimizer support|
|Reliability at scale	|Spark DAG & Lineage|	Automatic fault recovery|

## Code to Create DataFrame
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
# Initialize Spark session
spark = SparkSession.builder.appName("Movie Ratings Analysis").getOrCreate()
# Sample data
data = [
    (1, 101, 4, 1622388000000),
    (1, 102, 3, 1622388020000),
    (2, 101, 5, 1622388040000),
    (2, 103, 4, 1622388060000),
    (3, 101, 3, 1622388080000),
    (3, 102, 4, 1622388100000),
    (3, 103, None, 1622388120000),
    (4, 101, 2, 1622388140000)
]
# Create a DataFrame
columns = ["user_id", "movie_id", "rating", "timestamp"]
df = spark.createDataFrame(data, schema=columns)
df.show()
```

<img width="1123" height="349" alt="image" src="https://github.com/user-attachments/assets/9a6b4e31-443a-48cd-895b-d820b1665bf1" />

## Step 1: Filter
```python
#Find all ratings that are greater than or equal to 4.
df_filtered = df.filter(df.rating >= 4)
df_filtered.show()
```

<img width="1126" height="245" alt="image" src="https://github.com/user-attachments/assets/1923898a-8c39-4fe7-b1c9-17f25ef54892" />

## Step 2: Handle Null Values
```python
#Replace null values in the 'rating' column with the average rating.
average_rating = df.selectExpr('avg(rating)').collect()[0][0]
df_filled = df.na.fill({'rating': average_rating})
df_filled.show()
```

<img width="1131" height="345" alt="image" src="https://github.com/user-attachments/assets/399f0c0d-c711-4c8d-8aec-efe6cfe6d4a6" />

## Step 3: Drop Duplicates
```python
#Remove duplicate ratings by 'user_id' and 'movie_id'.
df_no_duplicates = df.dropDuplicates(['user_id', 'movie_id'])
df_no_duplicates.show()
```

<img width="1129" height="338" alt="image" src="https://github.com/user-attachments/assets/0b8e484f-5754-45d8-b2b8-564530f6c26e" />

## Step 4: Select Specific Columns
```python
#Select 'user_id' and 'rating' columns.
df_selected = df.select('user_id', 'rating')
df_selected.show()
```

<img width="1138" height="348" alt="image" src="https://github.com/user-attachments/assets/7438f587-335c-40d8-85ba-9176e7b3be0c" />

## Step 5: Grouping and Aggregating
```python
#Calculate the average rating per movie.
df_grouped = df.groupBy('movie_id').agg({'rating': 'avg'})
df_grouped.show()
```

<img width="1127" height="229" alt="image" src="https://github.com/user-attachments/assets/3adc9660-5d3c-4153-b6b4-c229adbd8321" />

## Step 6: Joining DataFrames
```python
#Join the 'movie_ratings' DataFrame with a 'movie_details' DataFrame that contains 'movie_id' and 'movie_name'.
#Assuming df_movies is another DataFrame that contains movie details
df_joined = df.join(df_movies, on='movie_id', how='inner')
df_joined.show()
```

<img width="813" height="1128" alt="image" src="https://github.com/user-attachments/assets/0fe22ea8-a0b3-4a52-b3da-4bc935cd589d" />

## Step 7: Union of DataFrames
```python
#Union this DataFrame with another 'df_new_ratings' containing additional ratings data.
df_union = df.union(df_new_ratings)
df_union.show()
```

<img width="648" height="204" alt="image" src="https://github.com/user-attachments/assets/74adeb32-0bd4-4b7d-87f2-fc7f1560b882" />

<img width="906" height="384" alt="image" src="https://github.com/user-attachments/assets/ecc41cbc-b33c-4bb5-85a1-58cd6d29c6c6" />

## Step 8: Temporary View and SQL
```python
Create a temp view and find the top-rated movies.
df.createOrReplaceTempView('ratings')
sql_result = spark.sql('SELECT movie_id, AVG(rating) as avg_rating FROM ratings GROUP BY movie_id ORDER BY avg_rating DESC LIMIT 10')
sql_result.show()
```
<img width="1128" height="228" alt="image" src="https://github.com/user-attachments/assets/6f9dc98d-0aa9-482b-8361-e8631a2e4ad8" />
