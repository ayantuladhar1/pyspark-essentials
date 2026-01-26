# City Temperature Analysis
## Problem Statement	
* As a data engineer you are supposed to prepare the data from temp analysis
* Your pipeline should return data in form of following columns
  * city
  * avg_temperature
  * total_temperature
  * num_measurements
* You should return metrics for only those cities when total_temperature is greater than 30
* And output should be sorted on city in ascending order

## Data
```csv
New York , 10.0  
New York , 12.0 
Los Angeles , 20.0  
Los Angeles , 22.0 
San Francisco , 15.0  
San Francisco , 18.0
```

## Metadata- columns
city - String  
temperature - Double  

## PySpark Solution
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import sum, avg, max, min, mean, count, col

#City Temperature Analysis
if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("BootcampApp") \
        .getOrCreate()

data = [("New York", 10.0),
        ("New York", 12.0),
         ("Los Angeles", 20.0 ),
        ("Los Angeles", 22.0),
        ("San Francisco", 15.0),
        ("San Francisco", 18.0)]

schema =  StructType([ \
    StructField("City",StringType(),True), \
    StructField("Temperature",DoubleType(),True), \
  ])

df = spark.createDataFrame(data=data,schema=schema)
#df.groupBy("City").count().show()
#df_avg=df.groupBy("City").mean("Temperature")
#df_total=df.groupBy("City").sum("Temperature")
#df.show(truncate=False)

city_temp = (df.groupBy("City")
.agg(
    avg("Temperature").alias("Avg_Temperature"),
    sum("Temperature").alias("Total_Temperature"),
    count("Temperature").alias("Num_Temperature")
    )
    .where(col("Total_Temperature") >= 30)
    .orderBy("City")
)

city_temp.show(truncate=False)
```
## Output

<img width="819" height="185" alt="image" src="https://github.com/user-attachments/assets/ea30e2c9-e673-4d46-89dc-a51cb48a1c59" />
