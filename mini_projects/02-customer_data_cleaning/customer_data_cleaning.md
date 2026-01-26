# Customer Data Cleaning
## Problem Statement
As a data engineer you are given the task to clean the customer data
Your pipeline should remove all the duplicates records
And also remove those records which are duplicated on the basis of height and age

## Data
```csv
Smith,23,5.3
Rashmi,27,5.8
Smith,23,5.3
Payal,27,5.8
Megha,27,5.4
```

## Metadata- columns
Name - String  
Age - Integer  
Height - double  

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType

## PySpark Solution
```python
#Customer Data Cleaning
if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("BootcampApp") \
        .getOrCreate()

data = [("Smith", 23, 5.3),
        ("Rashmi", 27, 5.8),
         ("Smith", 23, 5.3),
        ("Payal", 27, 5.8),
        ("Megha", 27, 5.4)]

schema =  StructType([ \
    StructField("Name",StringType(),True), \
    StructField("Age",IntegerType(),True), \
    StructField("Height",DoubleType(),True), \
  ])

df = spark.createDataFrame(data=data,schema=schema)
distinctDF = df.distinct()
df2 = distinctDF.dropDuplicates()
#print("Distinct count: "+str(distinctDF.count()))
print("Distinct count: "+str(df2.count()))
#distinctDF.show(truncate=False)
df2.show(truncate=False)
dropDisDF = df.dropDuplicates(["Age","Height"])
print("Distinct count of Age & Height : "+str(dropDisDF.count()))
dropDisDF.show(truncate=False)
```
## Output

<img width="833" height="408" alt="image" src="https://github.com/user-attachments/assets/3eee551f-0c21-42d7-8de3-edc5b96150e0" />
