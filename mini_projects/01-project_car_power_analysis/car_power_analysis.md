# Car Power Analysis
## Problem Statement
As a data engineer you are given the task to transform the given data
In addition to given metadata, transformed data should have a column named as AvgWeight with constant value as 200 and also kilowatt_power which needs to be 1000 times horsepower.
Column name carr is a mis-spelled column name and you are supposed to correct this to car

## Data
```csv
Ford Torino, 140, 3449, US
Chevrolet Monte Carlo, 150, 3761, US
BMW 2002, 113, 2234, Europe
```

## Metadata- columns
carr - String  
horsepower - Integer  
weight - Integer  
origin - String  

## PySpark Solution
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
#Car Project
if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("BootcampApp") \
        .getOrCreate()

data = [("Ford Torino", 140, 3449, "US"),
        ("Chevrolet Monte Carlo", 150, 3761, "US"),
         ("BMW 2002", 113, 2234, "Europe")]

schema =  StructType([ \
    StructField("carr",StringType(),True), \
    StructField("horsepower",IntegerType(),True), \
    StructField("weight",IntegerType(),True), \
    StructField("origin", StringType(), True), \
  ])

df = spark.createDataFrame(data=data,schema=schema)
df1=(df.withColumn("AvgWeight", lit(200))
.withColumn("kilowatt_power", col("horsepower") * 1000)
.withColumnRenamed("carr","car"))
df1.show(truncate=False)
```

## Output

<img width="828" height="201" alt="image" src="https://github.com/user-attachments/assets/6e478de7-359d-4510-bfd4-c81d56c80279" />
