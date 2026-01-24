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

Code to Create DataFrame

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


Step 1: Filter

Find all ratings that are greater than or equal to 4.
df_filtered = df.filter(df.rating >= 4)
df_filtered.show()


Step 2: Handle Null Values

Replace null values in the 'rating' column with the average rating.
average_rating = df.selectExpr('avg(rating)').collect()[0][0]
df_filled = df.na.fill({'rating': average_rating})
df_filled.show()


Step 3: Drop Duplicates

Remove duplicate ratings by 'user_id' and 'movie_id'.
df_no_duplicates = df.dropDuplicates(['user_id', 'movie_id'])
df_no_duplicates.show()


Step 4: Select Specific Columns

Select 'user_id' and 'rating' columns.
df_selected = df.select('user_id', 'rating')
df_selected.show()


Step 5: Grouping and Aggregating

Calculate the average rating per movie.
df_grouped = df.groupBy('movie_id').agg({'rating': 'avg'})
df_grouped.show()


Step 6: Joining DataFrames

Join the 'movie_ratings' DataFrame with a 'movie_details' DataFrame that contains 'movie_id' and 'movie_name'.
# Assuming df_movies is another DataFrame that contains movie details
df_joined = df.join(df_movies, on='movie_id', how='inner')
df_joined.show()





Step 7: Union of DataFrames

Union this DataFrame with another 'df_new_ratings' containing additional ratings data.
df_union = df.union(df_new_ratings)
df_union.show()




Step 8: Temporary View and SQL

Create a temp view and find the top-rated movies.
df.createOrReplaceTempView('ratings')
sql_result = spark.sql('SELECT movie_id, AVG(rating) as avg_rating FROM ratings GROUP BY movie_id ORDER BY avg_rating DESC LIMIT 10')
sql_result.show()

<img width="1173" height="7671" alt="image" src="https://github.com/user-attachments/assets/c966b9b7-b47a-4f2f-99c6-53928da544ac" />
