# Create DataFrame from RDD
## Using toDF() function
```python
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
```

## PySpark RDD’s toDF() method
It is used to create a DataFrame from the existing RDD. Since RDD doesn’t have columns, the DataFrame is created with default column names “_1” and “_2” as we have two columns.
```python
rdd = spark.sparkContext.parallelize(data)
dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()
```
<img width="272" height="103" alt="image" src="https://github.com/user-attachments/assets/4f8f9659-3a0b-4af8-8936-6211fffbbf34" />

If you want to provide column names to the DataFrame use the toDF() method with column names as arguments as shown below.
```python
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()
```
<img width="314" height="109" alt="image" src="https://github.com/user-attachments/assets/e7c47c0e-4b5f-4abb-b552-0c8b881e3b89" />

```python
dfFromRDD1.show() —> Action
```
<img width="231" height="193" alt="image" src="https://github.com/user-attachments/assets/33bd793c-0e59-4915-8174-387e5477dfbc" />

# PySpark StructType & StructField Explained with Examples
```python
import pyspark
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show()
```
<img width="485" height="427" alt="image" src="https://github.com/user-attachments/assets/fcc9ba52-274d-4d3c-bbbb-6f43701fb8ed" />

# Defining Nested StructType object struct
While working on DataFrame we often need to work with the nested struct column and this can be defined using StructType.
In the below example column “name” data type is StructType which is nested.
```python
structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
df2.printSchema()
df2.show(truncate=False)
```
<img width="370" height="449" alt="image" src="https://github.com/user-attachments/assets/c2b24e45-0435-4ca3-8b44-05a9645fcac9" />

# Using createDataFrame()
Using createDataFrame() from SparkSession is another way to create manually and it takes rdd object as an argument. and chain with toDF() to specify names to the columns.
```python
dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
dfFromRDD2.show()
```

# Using createDataFrame() from SparkSession
Calling createDataFrame() from SparkSession is another way to create PySpark DataFrame manually, it takes a list object as an argument. and chain with toDF() to specify names to the columns.
```python
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
dfFromData2 = spark.createDataFrame(data).toDF(*columns)
```


