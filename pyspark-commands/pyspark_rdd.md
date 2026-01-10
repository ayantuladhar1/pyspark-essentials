# RDD in Spark
RDD (Resilient Distributed Dataset) is the core, low-level data structure of Apache Spark.

* In simple terms:  
  * RDD = an immutable, distributed collection of data that Spark can process in parallel  

* Break the name down:  
  * **R – Resilient**
  * Fault-tolerant
  * If data is lost (node failure), Spark recomputes it automatically using lineage

  * **D – Distributed**
  * Data is split across multiple machines (nodes) in a cluster
  * Each node processes its own chunk in parallel

  * **D – Dataset**
  * A collection of elements (numbers, strings, objects, rows)

* Key Characteristics of RDD:
|Feature|	Explanation|
|-------|--------------|
|Immutable|	You can’t change an RDD; every transformation creates a new RDD|
|Distributed|	Stored across cluster nodes|
|Fault-tolerant|	Uses lineage (DAG) to recover data|
|Lazy evaluation|	Nothing runs until an action is called|
|Parallel processing|	Fast processing on large datasets|

# To Create RDD using sparkContext.parallelize()
* From existing data (HDFS, local, S3, etc.)
```python
from pyspark.sql import SparkSession
spark:SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com".getOrCreate())
```
* From in-memory collections
```python
data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd=spark.sparkContext.parallelize(data)
rdd.count()
```

# To Create RDD using sparkContext.textFile()
```python
rdd2 = spark.sparkContext.textFile("file:///home/takeo/customer.txt")
rdd2.count()
```

# Creating empty RDD with partition
```python
rdd10=spark.sparkContext.parallelize(data, 10)
print("initial partition count:"+str(rdd10.getNumPartitions()))
```

# Repartition and Coalesce
Sometimes we may need to repartition the RDD, PySpark provides two ways to repartition; first using repartition() method which shuffles data from all nodes also called full shuffle and second coalesce() method which shuffle data from minimum nodes, for examples if you have data in 4 partitions and doing coalesce(2) moves data from just 2 nodes.  
Both of the functions take the number of partitions to repartition rdd as shown below.  Note that the repartition() method is a very expensive operation as it shuffles data from all nodes in a cluster. 
```sql
reparRdd = rdd10.repartition(4)
print("re-partition count:"+str(reparRdd.getNumPartitions()))
```
# PySpark RDD Operations
* RDD transformations – Transformations are lazy operations, instead of updating an RDD, these operations return another RDD.
* RDD actions – operations that trigger computation and return RDD values.

# RDD Transformations with example
```python
rdd = spark.sparkContext.textFile("file:///home/takeo/test.txt")
```

# flatMap – flatMap() transformation flattens the RDD after applying the function and returns a new RDD. In the below example, first, it splits each record by space in an RDD and finally flattens it. Resulting RDD consists of a single word on each record.
```pyhton
rdd2 = rdd.flatMap(lambda x: x.split(" "))
```

# map – map() transformation is used to apply any complex operations like adding a column, updating a column e.t.c, the output of map transformations would always have the same number of records as input.
```python
rdd3 = rdd2.map(lambda x: (x,1))
```

# reduceByKey – reduceByKey() merges the values for each key with the function specified. In our example, it reduces the word string by applying the sum function on value. The result of our RDD contains unique words and their count.
```python
rdd5 = rdd3.reduceByKey(lambda a,b: a+b)
print(rdd5.collect())
```

# sortByKey – sortByKey() transformation is used to sort RDD elements on key. In our example, first, we convert RDD[(String,Int]) to RDD[(Int, String]) using map transformation and apply sortByKey which ideally does sort on an integer value. And finally, foreach with println statements returns all words in RDD and their count as key-value pair
```python
rdd6 = rdd5.map(lambda x: (x[1],x[0])).sortByKey()
print(rdd6.collect())
```

# filter – filter() transformation is used to filter the records in an RDD. In our example we are filtering all words contains “an”.
```python
rdd4 = rdd3.filter(lambda x : 'an' in x[0])
print(rdd4.collect())
```

# RDD Actions with example

RDD Action operations return the values from an RDD to a driver program. In other words, any RDD function that returns non-RDD is considered as an action

count() – Returns the number of records in an RDD
first() – Returns the first record.


print("Count : "+str(rdd6.count()))






# Action - first
firstRec = rdd6.first()
print("First Record : "+str(firstRec[0]) + ","+ firstRec[1])





max() – Returns max record.


# Action - max
datMax = rdd6.max()
print("Max Record : "+str(datMax[0]) + ","+ datMax[1])





reduce() – Reduces the records to single, we can use this to count or sum.




# Action - reduce
totalWordCount = rdd6.reduce(lambda a,b: (a[0]+b[0],a[1]))
print("dataReduce Record : "+str(totalWordCount[0]))





take() – Returns the record specified as an argument.


# Action - take
data3 = rdd6.take(3)
for f in data3:
    print("data3 Key:"+ str(f[0]) +", Value:"+f[1])





collect() – Returns all data from RDD as an array. Be careful when you use this action when you are working with huge RDD with millions and billions of data as you may run out of memory on the driver.



# Action - collect
data = rdd6.collect()
for f in data:
    print("Key:"+ str(f[0]) +", Value:"+f[1])





saveAsTextFile() – Using saveAsTestFile action, we can write the RDD to a text file.



rdd6.saveAsTextFile("file:///home/takeo/wordCount")






