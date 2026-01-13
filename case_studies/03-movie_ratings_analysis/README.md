# PySpark DataFrame Case Study: Movie Ratings Analysis
## Scenario: You have a dataset of movie ratings submitted by users. The goal is to analyze user preferences, average ratings, and identify popular movies. The dataset contains the following columns:
* user_id: ID of the user
* movie_id: ID of the movie
* rating: Rating given by the user (1-5 scale)
* timestamp: Time of the rating

## Sample Data
|user_id |movie_id |rating|timestamp          |
|--------|---------|------|-------------------|
|1       |101      |4     |1622388000000      |
|1       |102      |3     |1622388020000      |
|2       |101      |5     |1622388040000      |
|2       |103      |4     |1622388060000      |
|3       |101      |3     |1622388080000      |
|3       |102      |4     |1622388100000      |
|3       |103      |NULL  |1622388120000      |
|4       |101      |2     |1622388140000      |

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

## Filter
Find all ratings that are greater than or equal to 4.
```python
df_filtered = df.filter(df.rating >= 4)
df_filtered.show()
```

## Handle Null Values
Replace null values in the 'rating' column with the average rating.
```python
average_rating = df.selectExpr('avg(rating)').collect()[0][0]
df_filled = df.na.fill({'rating': average_rating})
df_filled.show()
```

## Drop Duplicates
Remove duplicate ratings by 'user_id' and 'movie_id'.
```python
df_no_duplicates = df.dropDuplicates(['user_id', 'movie_id'])
df_no_duplicates.show()
```

## Select Specific Columns
Select 'user_id' and 'rating' columns.
```python
df_selected = df.select('user_id', 'rating')
df_selected.show()
```

## Grouping and Aggregating
Calculate the average rating per movie.
```python
df_grouped = df.groupBy('movie_id').agg({'rating': 'avg'})
df_grouped.show()
```

## Joining DataFrames
Join the 'movie_ratings' DataFrame with a 'movie_details' DataFrame that contains 'movie_id' and 'movie_name'.
```python
# Assuming df_movies is another DataFrame that contains movie details
df_joined = df.join(df_movies, on='movie_id', how='inner')
df_joined.show()
```

## Union of DataFrames
Union this DataFrame with another 'df_new_ratings' containing additional ratings data.
```python
df_union = df.union(df_new_ratings)
df_union.show()
```

## Temporary View and SQL
Create a temp view and find the top-rated movies.
```python
df.createOrReplaceTempView('ratings')
sql_result = spark.sql('SELECT movie_id, AVG(rating) as avg_rating FROM ratings GROUP BY movie_id ORDER BY avg_rating DESC LIMIT 10')
sql_result.show()
```
