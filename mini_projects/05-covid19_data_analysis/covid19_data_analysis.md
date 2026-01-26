# Covid-19 Data Analysis
## Section 1
* Rename the column infection_case to infection_source
```python
df.withColumnRenamed("infection_case", "infection_source").show(truncate=False)
```

<img width="1190" height="635" alt="image" src="https://github.com/user-attachments/assets/fad31724-4da6-4bbf-a2a3-6fe7d765141d" />
	
* Select only following columns 
  * 'Province','city',infection_source,'confirmed'
```python
column_rename = df.withColumnRenamed("infection_case", "infection_source")
column_rename.select("province", "city", "infection_source", "confirmed").show()
```

<img width="1107" height="632" alt="image" src="https://github.com/user-attachments/assets/69b0b6ef-a0aa-4863-8454-9c04912bb0cd" />

* Change the datatype of confirmed column to integer
```python
schema = StructType([ \
    StructField("case_id", IntegerType(), True), \
    StructField("province", StringType(), True), \
    StructField("city", StringType(), True), \
    StructField("group", BooleanType(), True), \
    StructField("infection_case", StringType(), True), \
    StructField("confirmed", IntegerType(), True), \
    StructField("latitude", DoubleType(), True), \
    StructField("longitude", DoubleType(), True), \
])

df.select("confirmed").printSchema()
```

<img width="1077" height="129" alt="image" src="https://github.com/user-attachments/assets/ceb13a53-deb3-46e2-af21-82d796d661cd" />

* Return the TotalConfirmed and MaxFromOneConfirmedCase for each "province","city" pair
```python
df.groupBy("province", "city") \
    .agg(sum("confirmed").alias("TotalConfirmed"), \
         max("confirmed").alias("MaxFromOneConfirmedCase") \
         ) \
    .show(truncate=False)
```

<img width="919" height="617" alt="image" src="https://github.com/user-attachments/assets/fc0bb0b7-5349-40b3-b451-384c7b64007c" />

* Sort the output in asc order on the basis of confirmed
```python
order_asc = df.groupBy("province", "city") \
    .agg(sum("confirmed").alias("TotalConfirmed"), \
         max("confirmed").alias("MaxFromOneConfirmedCase") \
         )
order_asc.orderBy("TotalConfirmed", "MaxFromOneConfirmedCase").show()
```

<img width="938" height="605" alt="image" src="https://github.com/user-attachments/assets/3d46b83a-aa14-4725-b23f-c9fe79f21f53" />

## Section 2
* Return the top 2 provinces on the basis of confirmed cases.
```python
top2 = df.groupBy("province") \
   .agg(sum("confirmed").alias("TotalConfirmed"))
top2.show(2)
```

<img width="1087" height="207" alt="image" src="https://github.com/user-attachments/assets/cb85f1dd-ce5e-43c1-87c4-9fdf231d0452" />

## Section 3
* Return the details only for ‘Busan’ as province name where confirmed cases are more than 10
```python
df.where(
    (col("province") == "Busan") & \
    (col("confirmed") > 10)
).show()
```

<img width="1105" height="288" alt="image" src="https://github.com/user-attachments/assets/f6a5e8f0-5d7c-45c6-a3a0-af4c2bd43f47" />

* Select the columns other than latitude, longitude and case_id
```python
#Alternatives
#drop_list = ['latitude', 'longitude', 'case_id',]
#df = df.select([column for column in df.columns if column not in drop_list]

df.drop("latitude", "longitude", "case_id").show(truncate=False)
```

<img width="1077" height="618" alt="image" src="https://github.com/user-attachments/assets/d7e7b5a4-bb7e-4500-ab67-362ca8f2d205" />
