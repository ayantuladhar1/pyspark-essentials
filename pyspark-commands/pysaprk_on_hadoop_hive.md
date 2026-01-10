Create a file with name as user.txt and copy below data into that file and put the file on hadoop.

users.txt 
3002,Nick Rimando,New York,100,5001
3005,Graham Zusi,California,200,5002
3001,Brad Guzan,London,,5005
3004,Fabian Johns,Paris,300,5006
3007,Brad Davis,New York,200,5001
3009,Geoff Camero,Berlin,100,5003
3008,Julian Green,London,300,5002
3003,Jozy Altidor,Moscow,200,5007




Start Hadoop

~/startApps/restartHadoop.sh



Command to put file on hadoop

hadoop fs -mkdir -p /data/spark/test
hadoop fs -put ~/users.txt /data/spark/test


Create RDD over hadoop file


rdd = sc.textFile("/data/spark/test/users.txt")
rdd.count()
rdd.collect()


Create Dataframe over hadoop file

Create a file with name as zipcodes1.csv for below data and put the file on hadoop 

zipcodes1.csv
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
76512,27203,STANDARD,ASHEBORO,NC,PRIMARY,35.71,-79.81,0.14,-0.79,0.58,NA,US,"Asheboro, NC",NA-US-NC-ASHEBORO,FALSE,8355,15228,215474318,
76513,27204,PO BOX,ASHEBORO,NC,PRIMARY,35.71,-79.81,0.14,-0.79,0.58,NA,US,"Asheboro, NC",NA-US-NC-ASHEBORO,FALSE,1035,1816,30322473,







hadoop fs -put $HOME/zipcodes1.csv /data/spark/test


Create dataframe

df = spark.read.csv("/data/spark/test/zipcodes1.csv")
df.printSchema()
df.show()


Write this dataframe to hadoop into parquet file

df.write.parquet("/data/spark/test/parquet/people.parquet")


Read parquet file into dataframe from hadoop

parDF=spark.read.parquet("/data/spark/test/parquet/people.parquet")
parDF.show()





*********************** Spark On Hive ************************

start hive

~/startApps/restartHive.sh



df3 = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .csv("/data/spark/test/zipcodes1.csv")

df3.printSchema()
df3.show()


Write dataframe to hive table with file format as text (default hive table format)
Here default is a database name in hive
zip_table is a table name in database default

takeo@dbf9a19cecc1:~$ sudo nano /etc/hosts


df3.write.mode("overwrite").saveAsTable("default.zip_table")




Note : You can change this default database and table name accordingly 

Read already created table from hive and create dataframe

Here default is a database name in hive
zip_table is a table name in database default

tabDF = spark.sql("select * from default.zip_table")
tabDF.show()




1. Write to partitioned hive table with file format as text (default hive table format)

Partition column name in table is city
Table Name in hive will be city_part

spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

df3.write.mode("overwrite").partitionBy("city").saveAsTable("default.city_part")



Note : You can change this default database and table name accordingly 
2. Write to partitioned hive table with file format as text (default hive table format)
Partition column names are state and city
Table Name in hive will be state_city
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

df3.write.mode("overwrite").partitionBy("state","city").saveAsTable("default.state_city")




Note : You can change this default database and table name accordingly 

3. Write to partitioned hive table with file format as parquet 

Partition column names are state and city
Table Name in hive will be state_city_parquet


spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
df3.write.mode("overwrite").partitionBy("state","city").format("parquet").saveAsTable("default.state_city_parquet")



Note : You can change this default database and table name accordingly 

Go to hive console and run 

show tables;


You will find all these tables in default database 

Try some sql’s on these hive table

Select only city column when city is ASHEBORO
sdf = spark.sql("select city from default.state_city_parquet where city='ASHEBORO'")
sdf.printSchema()
sdf.show()



Select all columns when city is MESA and State is AZ
sdf = spark.sql("select * from default.state_city_parquet where city='MESA' and state= 'AZ' ")
sdf.printSchema()
sdf.show()


