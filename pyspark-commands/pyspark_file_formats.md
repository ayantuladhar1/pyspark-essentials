# Create zipcodes.csv in your home dir.
```csv
RecordNumber,Zipcode,ZipCodeType,City,State,LocationType,Lat,Long,Xaxis,Yaxis,Zaxis,WorldRegion,Country,LocationText,Location,Decommisioned,TaxReturnsFiled,EstimatedPopulation,TotalWages,Notes
1,704,STANDARD,PARC PARQUE,PR,NOT ACCEPTABLE,17.96,-66.22,0.38,-0.87,0.3,NA,US,"Parc Parque, PR",NA-US-PR-PARC PARQUE,FALSE,,,,
2,704,STANDARD,PASEO COSTA DEL SUR,PR,NOT ACCEPTABLE,17.96,-66.22,0.38,-0.87,0.3,NA,US,"Paseo Costa Del Sur, PR",NA-US-PR-PASEO COSTA DEL SUR,FALSE,,,,
10,709,STANDARD,BDA SAN LUIS,PR,NOT ACCEPTABLE,18.14,-66.26,0.38,-0.86,0.31,NA,US,"Bda San Luis, PR",NA-US-PR-BDA SAN LUIS,FALSE,,,,
61391,76166,UNIQUE,CINGULAR WIRELESS,TX,NOT ACCEPTABLE,32.72,-97.31,-0.1,-0.83,0.54,NA,US,"Cingular Wireless, TX",NA-US-TX-CINGULAR WIRELESS,FALSE,,,,
61392,76177,STANDARD,FORT WORTH,TX,PRIMARY,32.75,-97.33,-0.1,-0.83,0.54,NA,US,"Fort Worth, TX",NA-US-TX-FORT WORTH,FALSE,2126,4053,122396986,
61393,76177,STANDARD,FT WORTH,TX,ACCEPTABLE,32.75,-97.33,-0.1,-0.83,0.54,NA,US,"Ft Worth, TX",NA-US-TX-FT WORTH,FALSE,2126,4053,122396986,
4,704,STANDARD,URB EUGENE RICE,PR,NOT ACCEPTABLE,17.96,-66.22,0.38,-0.87,0.3,NA,US,"Urb Eugene Rice, PR",NA-US-PR-URB EUGENE RICE,FALSE,,,,
39827,85209,STANDARD,MESA,AZ,PRIMARY,33.37,-111.64,-0.3,-0.77,0.55,NA,US,"Mesa, AZ",NA-US-AZ-MESA,FALSE,14962,26883,563792730,"no NWS data, "
39828,85210,STANDARD,MESA,AZ,PRIMARY,33.38,-111.84,-0.31,-0.77,0.55,NA,US,"Mesa, AZ",NA-US-AZ-MESA,FALSE,14374,25446,471000465,
49345,32046,STANDARD,HILLIARD,FL,PRIMARY,30.69,-81.92,0.12,-0.85,0.51,NA,US,"Hilliard, FL",NA-US-FL-HILLIARD,FALSE,3922,7443,133112149,
49346,34445,PO BOX,HOLDER,FL,PRIMARY,28.96,-82.41,0.11,-0.86,0.48,NA,US,"Holder, FL",NA-US-FL-HOLDER,FALSE,,,,
49347,32564,STANDARD,HOLT,FL,PRIMARY,30.72,-86.67,0.04,-0.85,0.51,NA,US,"Holt, FL",NA-US-FL-HOLT,FALSE,1207,2190,36395913,
49348,34487,PO BOX,HOMOSASSA,FL,PRIMARY,28.78,-82.61,0.11,-0.86,0.48,NA,US,"Homosassa, FL",NA-US-FL-HOMOSASSA,FALSE,,,,
10,708,STANDARD,BDA SAN LUIS,PR,NOT ACCEPTABLE,18.14,-66.26,0.38,-0.86,0.31,NA,US,"Bda San Luis, PR",NA-US-PR-BDA SAN LUIS,FALSE,,,,
3,704,STANDARD,SECT LANAUSSE,PR,NOT ACCEPTABLE,17.96,-66.22,0.38,-0.87,0.3,NA,US,"Sect Lanausse, PR",NA-US-PR-SECT LANAUSSE,FALSE,,,,
54354,36275,PO BOX,SPRING GARDEN,AL,PRIMARY,33.97,-85.55,0.06,-0.82,0.55,NA,US,"Spring Garden, AL",NA-US-AL-SPRING GARDEN,FALSE,,,,
54355,35146,STANDARD,SPRINGVILLE,AL,PRIMARY,33.77,-86.47,0.05,-0.82,0.55,NA,US,"Springville, AL",NA-US-AL-SPRINGVILLE,FALSE,4046,7845,172127599,
54356,35585,STANDARD,SPRUCE PINE,AL,PRIMARY,34.37,-87.69,0.03,-0.82,0.56,NA,US,"Spruce Pine, AL",NA-US-AL-SPRUCE PINE,FALSE,610,1209,18525517,
76511,27007,STANDARD,ASH HILL,NC,NOT ACCEPTABLE,36.4,-80.56,0.13,-0.79,0.59,NA,US,"Ash Hill, NC",NA-US-NC-ASH HILL,FALSE,842,1666,28876493,
76512,27203,STANDARD,ASHEBORO,NC,PRIMARY,35.71,-79.81,0.14,-0.79,0.58,NA,Ucat S,"Asheboro, NC",NA-US-NC-ASHEBORO,FALSE,8355,15228,215474318,
76513,27204,PO BOX,ASHEBORO,NC,PRIMARY,35.71,-79.81,0.14,-0.79,0.58,NA,US,"Asheboro, NC",NA-US-NC-ASHEBORO,FALSE,1035,1816,30322473,
```
# Read the csv file uisng Pyspark
```python
df = spark.read.csv("file:///home/takeo/zipcodes.csv")
df.printSchema()
```

```python
df.show()
```

# Using Header Record For Column Names
```python
df2 = spark.read.option("header",True).csv("file:///home/takeo/zipcodes.csv")
```

As mentioned earlier, PySpark reads all columns as a string (StringType) by default.

# Options While Reading CSV File
```python
df3 = spark.read.options(delimiter=',').csv("file:///home/takeo/zipcodes.csv")
```

# InferSchema
The default value set to this option is False when setting to true it automatically infers column types based on the data. Note that, it requires reading the data one more time to infer the schema.
```python
df4 = spark.read.options(inferSchema='True',delimiter=',') \
  .csv("file:///home/takeo/zipcodes.csv")
```
## Another way
```python
df4 = spark.read.option("inferSchema",True) \
                .option("delimiter",",") \
  .csv("file:///home/takeo/zipcodes.csv")
```

# header
This option is used to read the first line of the CSV file as column names. By default the value of this option is False , and all column types are assumed to be a string.
```python
df3 = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .csv("file:///home/takeo/zipcodes.csv")
df3.printSchema()
```

# Reading CSV files with a user-specified custom schema
If you know the schema of the file ahead and do not want to use the inferSchema option for column names and types
```python
import pyspark
from pyspark.sql.types import *
schema = StructType() \
      .add("RecordNumber",IntegerType(),True) \
      .add("Zipcode",IntegerType(),True) \
      .add("ZipCodeType",StringType(),True) \
      .add("City",StringType(),True) \
      .add("State",StringType(),True) \
      .add("LocationType",StringType(),True) \
      .add("Lat",DoubleType(),True) \
      .add("Long",DoubleType(),True) \
      .add("Xaxis",IntegerType(),True) \
      .add("Yaxis",DoubleType(),True) \
      .add("Zaxis",DoubleType(),True) \
      .add("WorldRegion",StringType(),True) \
      .add("Country",StringType(),True) \
      .add("LocationText",StringType(),True) \
      .add("Location",StringType(),True) \
      .add("Decommisioned",BooleanType(),True) \
      .add("TaxReturnsFiled",StringType(),True) \
      .add("EstimatedPopulation",IntegerType(),True) \
      .add("TotalWages",IntegerType(),True) \
      .add("Notes",StringType(),True)
      
df_with_schema = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("file:///home/takeo/zipcodes.csv")
````

# Saving modes
PySpark DataFrameWriter also has a method mode() to specify saving mode.
overwrite – mode is used to overwrite the existing file.
```python
df_with_schema.write.mode('overwrite').csv("file:///tmp/spark_output/zipcodes")
# you can also use this
df_with_schema.write.format("csv").mode('overwrite').save("file:///tmp/spark_output/zipcodes")
```
# Parquet
## Pyspark Write DataFrame to Parquet file format
```python
data =[("James ","","Smith","36636","M",3000),
              ("Michael ","Rose","","40288","M",4000),
              ("Robert ","","Williams","42114","M",4000),
              ("Maria ","Anne","Jones","39192","F",4000),
              ("Jen","Mary","Brown","","F",-1)]
columns=["firstname","middlename","lastname","dob","gender","salary"]
df=spark.createDataFrame(data,columns)
df.write.parquet("file:///tmp/output/people.parquet")
```

## Read Parquet file into DataFrame
```python
parDF=spark.read.parquet("file:///tmp/output/people.parquet")
```

## SQL queries DataFrame
```python
parDF.createOrReplaceTempView("ParquetTable")
parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")
parkSQL.show()
```


# ORC
```python
parDF.write.orc("file:///tmp/orc/data.orc")
```
```python
df = spark.read.orc("file:///tmp/orc/data.orc")
df.printSchema()
df.show()
```

## Run sql on Orc File
```python
df.createOrReplaceTempView("ORCTable")
orcSQL = spark.sql("select firstname,dob from ORCTable where salary >= 4000 ")
orcSQL.show()
```

# JSON
```python
# Read JSON file into dataframe
parDF.write.json("file:///tmp/json/data.json")
df = spark.read.json("file:///tmp/json/data.json")
df.printSchema()
df.show()
```
