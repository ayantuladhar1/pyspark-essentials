# What is PySpark Partition?
PySpark partition is a way to split a large dataset into smaller datasets based on one or more partition keys. When you create a DataFrame from a file/table, based on certain parameters PySpark creates the DataFrame with a certain number of partitions in memory. This is one of the main advantages of PySpark DataFrame over Pandas DataFrame. Transformations on partitioned data run faster as they execute transformations parallelly for each partition.
PySpark supports partition in two ways; partition in memory (DataFrame) and partition on the disk (File system).
Partition in memory: You can partition or repartition the DataFrame by calling repartition() or coalesce() transformations.
Partition on disk: While writing the PySpark DataFrame back to disk, you can choose how to partition the data based on columns using partitionBy() of pyspark.sql.DataFrameWriter. This is similar to Hives partition scheme.

# Partition Advantages
As you are aware PySpark is designed to process large datasets 100x faster than the traditional processing, this wouldn’t have been possible without partition. Below are some of the advantages of using PySpark partitions on memory or on disk.
Fast accessed to the data
Provides the ability to perform an operation on a smaller dataset
Partition at rest (disk) is a feature of many databases and data processing frameworks and it is key to make jobs work at scale.

## Create a file simple-zipcodes.csv for below data
```csv
RecordNumber,Country,City,Zipcode,State
1,US,PARC PARQUE,704,PR
2,US,PASEO COSTA DEL SUR,704,PR
10,US,BDA SAN LUIS,709,PR
49347,US,HOLT,32564,FL
49348,US,HOMOSASSA,34487,FL
61391,US,CINGULAR WIRELESS,76166,TX
61392,US,FORT WORTH,76177,TX
61393,US,FT WORTH,76177,TX
54356,US,SPRUCE PINE,35585,AL
76511,US,ASH HILL,27007,NC
4,US,URB EUGENE RICE,704,PR
39827,US,MESA,85209,AZ
39828,US,MESA,85210,AZ
49345,US,HILLIARD,32046,FL
49346,US,HOLDER,34445,FL
3,US,SECT LANAUSSE,704,PR
54354,US,SPRING GARDEN,36275,AL
54355,US,SPRINGVILLE,35146,AL
76512,US,ASHEBORO,27203,NC
76513,US,ASHEBORO,27204,NC
```
```python
df=spark.read.option("header",True) \
        .csv("file:///home/takeo/simple-zipcodes.csv")
df.printSchema()
```

When you write PySpark DataFrame to disk by calling partitionBy(), PySpark splits the records based on the partition column and stores each partition data into a sub-directory.

## partitionBy()
```python
df.write.option("header",True) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("file:///tmp/parts/zipcodes-state")
df.write.option("header",True).partitionBy("state").mode("overwrite").csv("file:///tmp/parts/simple-zipcodes-state")
```

On our DataFrame, we have a total of 6 different states hence, it creates 6 directories as shown below. The name of the sub-directory would be the partition column and its value (partition column=value).

## partitionBy() Multiple Columns
```python
#partitionBy() multiple columns
df.write.option("header",True) \
        .partitionBy("state","city") \
        .mode("overwrite") \
        .csv("file:///tmp/parts/zipcodes-city-state")
```

## Using repartition() and partitionBy() together
For each partition column, if you wanted to further divide into several partitions, use repartition() and partitionBy() together as explained in the below example.
repartition() creates a specified number of partitions in memory. The partitionBy()  will write files to disk for each memory partition and partition column. 
```python
#Use repartition() and partitionBy() together
df.repartition(2).write.option("header",True).partitionBy("state").mode("overwrite").csv("file:///tmp/parts/zipcodes-state-more")
```

## Data Skew – Control Number of Records per Partition File
Use option maxRecordsPerFile if you want to control the number of records for each partition. This is particularly helpful when your data is skewed (Having some partitions with very low records and other partitions with high number of records).
```python
#partitionBy() control number of partitions
df.write.option("header",True) \
        .option("maxRecordsPerFile", 2) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("file:///tmp/parts/multi-zipcodes-state")
```

## Read a Specific Partition
Reads are much faster on partitioned data. This code snippet retrieves the data from a specific partition "state=AL and city=SPRINGVILLE". Here, It just reads the data from that specific folder instead of scanning a whole file (when not partitioned).
```python
dfSinglePart=spark.read.option("header",True) \
            .csv("file:////tmp/parts/zipcodes-city-state/state=AL/city=SPRINGVILLE")
dfSinglePart.printSchema()
dfSinglePart.show()
```

# PySpark SQL – Read Partition Data
```pythom
parqDF = spark.read.option("header",True) \
                  .csv("file:////tmp/parts/zipcodes-state/")
parqDF.createOrReplaceTempView("ZIPCODE")
spark.sql("select * from ZIPCODE  where state='AL' and city = 'SPRINGVILLE'") \
    .show()
```

# ArrayType
You can create an instance of an ArrayType using ArraType() class, This takes arguments valueType and one optional argument valueContainsNull to specify if a value can accept null, by default it takes True. valueType should be a PySpark type that extends DataType class.

## ArrayType Column Using StructType
This snippet creates two Array columns languagesAtSchool and languagesAtWork which defines languages learned at School and languages using at work. 
```python
data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

from pyspark.sql.types import StringType, ArrayType,StructType,StructField
schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("languagesAtSchool",ArrayType(StringType()),True), 
    StructField("languagesAtWork",ArrayType(StringType()),True), 
    StructField("currentState", StringType(), True), 
    StructField("previousState", StringType(), True)
  ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show()
```

## explode()
Use explode() function to create a new row for each element in the given array column. 
```python
from pyspark.sql.functions import explode
df.select(df.name,explode(df.languagesAtSchool)).show()
```

## Split()
split() sql function returns an array type after splitting the string column by delimiter. 
```python
from pyspark.sql.functions import split
df.select(split(df.name,",").alias("nameAsArray")).show()
```

## array()
Use array() function to create a new array column by merging the data from multiple columns. All input columns must have the same data type. The below example combines the data from currentState and previousState and creates a new column states.
```python
from pyspark.sql.functions import array
df.select(df.name,array(df.currentState,df.previousState).alias("States")).show()
```

## array_contains()
array_contains() sql function is used to check if array column contains a value. Returns null if the array is null, true if the array contains the value, and false otherwise.
```python
from pyspark.sql.functions import array_contains
df.select(df.name,array_contains(df.languagesAtSchool,"Java") .alias("array_contains")).show()
```

# MapType (Dict)
```python
from pyspark.sql.types import StringType, MapType
mapCol = MapType(StringType(),StringType(),False)
```

## MapType Key Points:
* The First param keyType is used to specify the type of the key in the map.
* The Second param valueType is used to specify the type of the value in the map.
* Third parm valueContainsNull is an optional boolean type that is used to specify if the value of the second param can accept Null/None values.
* The key of the map won’t accept None/Null values.
* PySpark provides several SQL functions to work with MapType.
* Create MapType From StructType
```python
from pyspark.sql.types import StructField, StructType, StringType, MapType
schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)
])

dataDictionary = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]
df = spark.createDataFrame(data=dataDictionary, schema = schema)
df.printSchema()
df.show(truncate=False)
```

# PySpark MapType Elements
Let’s see how to extract the key and values from the PySpark DataFrame Dictionary column. Here I have used PySpark map transformation to read the values of properties (MapType column)
```python
df3=df.rdd.map(lambda x: \
    (x.name,x.properties["hair"],x.properties["eye"])) \
    .toDF(["name","hair","eye"])
df3.printSchema()
df3.show()
```

Let’s use another way to get the value of a key from Map using getItem() of Column type, this method takes a key as an argument and returns a value.
```python
df.withColumn("hair",df.properties.getItem("hair")) \
  .withColumn("eye",df.properties.getItem("eye")) \
  .drop("properties") \
  .show()

df.withColumn("hair",df.properties["hair"]) \
  .withColumn("eye",df.properties["eye"]) \
  .drop("properties") \
  .show()
```

## explode
```python
from pyspark.sql.functions import explode
df.select(df.name,explode(df.properties)).show()
map_keys() – Get All Map Keys
```
```python
from pyspark.sql.functions import map_keys
df.select(df.name,map_keys(df.properties)).show()
```

In case you wanted to get all map keys as Python List. 
```python
from pyspark.sql.functions import explode,map_keys
keysDF = df.select(explode(map_keys(df.properties))).distinct()
keysList = keysDF.rdd.map(lambda x:x[0]).collect()
print(keysList)
#['eye', 'hair']
```

## map_values() – Get All map Values
```python
from pyspark.sql.functions import map_values
df.select(df.name,map_values(df.properties)).show()
```
