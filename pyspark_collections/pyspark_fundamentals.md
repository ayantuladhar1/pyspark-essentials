# What is PySpark?
PySpark is the Python API for Apache Spark, a distributed data processing engine designed for big data analytics. It allows you to process massive datasets in parallel across clusters using Python.

* In simple terms:
  * Spark = Big data processing engine.
  * PySpark = Spark + Python

* PySpark is widely used in Data Engineering, Data Analytics, and Machine Learning pipelines.

# Why PySpark is Important?
* PySpark is important because it:
   * Handles big data (TBs-PBs)
   * Is much faster than MapReduce (in-memory processing)
   * Supports batch + streaming
   * Integrates with HDFS, Hive, S3, ADLS, Kafka
   * Is heavily used in industry data pipelines

# PySpark Architecture
* Driver Program (Python) -> Spark Session -> Executors(Workers) -> Distributed Data Processing
* Key Components:
  * Driver - runs your PySpark code
  * Executors - execute tasks on cluster nodes
  * Cluster Manager - YARN/Kubernetes/Standalone

# PySpark vs Hadoop vs Hive

|Feature| Hadoop MapReduce|	Hive|	PySpark|
|-------|-----------------|-----|--------|
|Speed	|Slow	|Medium	|Very Fast|
|Language|	Java	|SQL	|Python / SQL|
|Processing|	Disk-based	|SQL on Hadoop	|In-memory|
|Use Case|	Legacy batch	|BI / Reporting	|Modern DE pipelines|

* Industry prefers PySpark, but it still uses Hadoop and Hive underneath.

# SparkSession(Entry Point)
* SparkSession is the starting point of PySpark
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
.appName("PySpark Basics") \
.getOrCreate()
```
# Data Abstractions in PySpark
* RDD (Resilient Distributed Dataset)
  * Low-level
  * Immutable
  * Fault-tolerant
```python
rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
```
* DataFrame
  * Structured data
  * Optimized with Catalyst Optimizer
  * SQL-like operations
```python
df = spark.read.csv("data.csv", header=True)
```
* DataSet
  * JVM-only(Scala/Java)

7. Reading Data in PySpark
  ○ CSV:
  df = spark.read.option("header", True).csv("/path/file.csv")
  ○ Parquet:
  df = spark.read.parquet("/path/file.parquet")
  ○ JSON:
  df = spark.read.json("/path/file.json")
  ○ Hive Table:
  df = spark.sql("SELECT * FROM sales")

8. Writing Data
df.write.mode("overwrite").parquet("/output/path")
Write formats:
  ○ CSV
  ○ Parquet(Preferred)
  ○ ORC
  ○ Hive Tables

9. Common DataFrame Operations
Select Columns:
df.select("name", "salary")

Filter Rows:
df.filter(df.salary > 50000)

Add Column
df.filter(df.salary > 50000)

Drop Column
df.drop("bonus")

10. Aggregate and GroupBy
Common Functions:
  ○ Sum
  ○ Avg
  ○ Count
  ○ Min/Max

11. Joins in PySpark
df1.join(df2, df1.id == df2.id, "inner")
Join Types:
  ○ Inner
  ○ Left
  ○ Right
  ○ Full

12. SparkSQL
You can query DataFrames using SQL
df.createOrReplaceTempView("employees")
spark.sql("SELECT department, AVG(salary) FROM employees GROUP BY department")

13. Partitioning and Performance
Repartition
df.repartition(4)
Coalesce
df.coalesce(1)

Why partitioning matters:
  ○ Parallelism
  ○ Performance
  ○ Cost optimization

14. PySpark with Hive
spark = SparkSession.builder \
.enableHiveSupport() \
.getOrCreate()

PySpark can:
  ○ Read Hive tables
  ○ Write partitioned Hive tables
  ○ Replace many Hive ETL jobs

15. PySpark in Real Data Engineering
Used for:
  ○ ETL pipelines
  ○ Data lake processing
  ○ Batch transformations
  ○ Feature engineering
  ○ Big joins and aggregations

Common stack:
  ○ PySpark + HDFS/S3
  ○ PySpark + Hive
  ○ PySpark + Airflow
  ○ PySpark + AWS Glue

16. PySpark vs Pandas

Feature	Pandas	PySpark
Data Size	Small	Huge
Processing	Single machine	Distributed
Speed	Limited	Very fast

Use Pandas for small data, PySpark for big data.

17. Best Practices
  ○ Use Parquet format
  ○ Avoid collect() on large data
  ○ Use DataFrames instead of RDDs
  ○ Partition wisely
  ○ Cache only when needed

18. Summary
PySpark is a core skill for modern Data Engineers. It stirs on top of Hadoop and Hive but provides:
  ○ Better performance
  ○ Simpler APIs
  ○ Industry relevance
