# Union
union() and unionAll() transformations are used to merge two or more DataFrame’s of the same schema or structure.

## Dataframe union()
* union() method of the DataFrame is used to merge two DataFrames of the same structure/schema. If schemas are not the same it returns an error.

## DataFrame unionAll()
* unionAll() is deprecated since Spark “2.0.0” version and replaced with union().
Note: In other SQL languages, Union eliminates the duplicates but UnionAll merges two datasets including duplicate records. But, in PySpark both behave the same and recommend using DataFrame duplicate() function to remove duplicate rows.

## First DataFrame
```python
import pyspark
simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]
columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)
```

## Second DataFrame
Now, let’s create a second Dataframe with the new records and some records from the above Dataframe but with the same schema.
```python
simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns2= ["employee_name","department","state","salary","age","bonus"]
df2 = spark.createDataFrame(data = simpleData2, schema = columns2)
df2.printSchema()
df2.show(truncate=False)
```

## Merge two or more DataFrames using union
DataFrame union() method merges two DataFrames and returns the new DataFrame with all rows from two Dataframes regardless of duplicate data.
```python
unionDF = df.union(df2)
unionDF.show(truncate=False)
```

## Merge without Duplicates
Since the union() method returns all rows without distinct records, we will use the distinct() function to return just one record when duplicate exists.
```python
disDF = df.union(df2).distinct()
disDF.show(truncate=False)
```
# PySpark map (map())
## Loop Through Rows in DataFrame
It is an RDD transformation that is used to apply the transformation function (lambda) on every element of RDD/DataFrame and returns a new RDD. In this article, you will learn the syntax and usage of the RDD map() transformation with an example and how to use it with DataFrame.

Note1: DataFrame doesn’t have map() transformation to use with DataFrame hence you need to DataFrame to RDD first.
Note2: If you have a heavy initialization use PySpark mapPartitions() transformation instead of map(), as with mapPartitions() heavy initialization executes only once for each partition instead of every record.
```python
data = ["Project","Gutenberg’s","Alice’s","Adventures",
"in","Wonderland","Project","Gutenberg’s","Adventures",
"in","Wonderland","Project","Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)
```
```python
rdd2=rdd.map(lambda x: (x,1))
for element in rdd2.collect():
    print(element)
```

# map() Example with DataFrame
PySpark DataFrame doesn’t have map() transformation to apply the lambda function, when you wanted to apply the custom transformation, you need to convert the DataFrame to RDD and apply the map() transformation.
```python
data = [('James','Smith','M',30),
  ('Anna','Rose','F',41),
  ('Robert','Williams','M',62), 
]
columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()
```
```python
# Refering columns by index.
rdd2=df.rdd.map(lambda x: 
    (x[0]+","+x[1],x[2],x[3]*2)
    )  
df2=rdd2.toDF(["name","gender","new_salary"]   )
df2.show()
```

Note that aboveI have used index to get the column values, alternatively, you can also refer to the DataFrame column names while iterating.
## Referring Column Names
```python
rdd2=df.rdd.map(lambda x: 
    (x["firstname"]+","+x["lastname"],x["gender"],x["salary"]*2)
    ) 
rdd2.collect()
```

## Referring Column Names
```python
rdd2=df.rdd.map(lambda x: 
    (x.firstname+","+x.lastname,x.gender,x.salary*2)
    ) 
rdd2.collect()
```

You can also create a custom function to perform an operation. Below func1() function executes for every DataFrame row from the lambda function.

## By Calling function
```python
def func1(x):
    firstName=x.firstname
    lastName=x.lastname
    name=firstName+","+lastName
    gender=x.gender.lower()
    salary=x.salary*2
    return (name,gender,salary)

rdd2=df.rdd.map(lambda x: func1(x))
rdd2.collect()
```


## Using foreach() to Loop Through Rows in DataFrame
Similar to map(), foreach() also applied to every row of DataFrame, the difference being foreach() is an action and it returns nothing. Below are some examples to iterate through DataFrame using for each.

## Foreach example
```python
def f(x): print(x)
df.foreach(f)
```

## Another example
```python
df.foreach(lambda x: 
    print("Data ==>"+x["firstname"]+","+x["lastname"]+","+x["gender"]+","+str(x["salary"]*2))
    ) 
```
