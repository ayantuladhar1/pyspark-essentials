# Books Data Analysis

## Create DF on json
```python
df= spark.read.json("hdfs:///case-study/books.json")
df.printSchema()
```

<img width="834" height="494" alt="image" src="https://github.com/user-attachments/assets/232945ed-ce92-44ea-867a-f489518f284d" />

## Counts the number of rows in dataframe
```python
print(df.count())
```

<img width="818" height="122" alt="image" src="https://github.com/user-attachments/assets/07393966-0a74-4baf-87a5-b3cbe2a19e43" />

## Counts the number of distinct rows in dataframe
```python
print(df.distinct())
print(df.distinct().count())
```

<img width="827" height="213" alt="image" src="https://github.com/user-attachments/assets/1fddc2c4-de77-4e4e-95cd-e36567cd81eb" />

## Remove Duplicate Values
```python
print("Total Rows", df.count())
df_books_duplicate = df.dropDuplicates(["title", "author"])
print ("Unique Books", df_books_duplicate.count())
df_weekly_title_duplicate =df.dropDuplicates(["title", "bestsellers_date"])
print("Unique Weekly Titles", df_weekly_title_duplicate.count())
```

<img width="830" height="151" alt="image" src="https://github.com/user-attachments/assets/c6a54d31-d248-43f1-aeae-36ad24245043" />

## Select title and assign 0 or 1 depending on title when title is not odd ODD HOURS and assign this value to a column named as newHours
```python
df.select("title", when(df.title != 'ODD HOURS', 1).otherwise(0).alias("newHours")).show(10)
```

<img width="825" height="355" alt="image" src="https://github.com/user-attachments/assets/731b5684-9535-4046-9443-b431c9f76fdb" />

## Select author and title is TRUE if title has "THE" word in titles and assign this value to a column named as universal
```python
title = df.select("author","title",(upper(col("title")).contains("THE")).alias("universal"))
title.show()
```

<img width="854" height="568" alt="image" src="https://github.com/user-attachments/assets/b75fc470-23e8-4f7c-b751-1031eebefbed" />

## Select substring of author from 1 to 3 and alias as newTitle1
```python
df.select(df.author.substr(1, 3).alias("newTitle1")).show(5)
```

<img width="845" height="242" alt="image" src="https://github.com/user-attachments/assets/3ec13fbe-c457-4101-9892-66b39b559659" />

## Select substring of author from 3 to 6 and alias as newTitle2
```python
df.select(df.author.substr(3, 6).alias("newTitle2")).show(5)
```

<img width="844" height="241" alt="image" src="https://github.com/user-attachments/assets/e2621a8f-ac44-4ad0-acbe-3a15c1bd4b42" />

## Show and Count all entries in title, author, rank, price columns
```python
df.select("title", "author", "rank", "price").show(truncate=False)
```

<img width="867" height="951" alt="image" src="https://github.com/user-attachments/assets/42c3c94e-d001-4396-a881-29c9e30502ed" />

## Show rows with for specified authors ("John Sandford", "Emily Giffin")
```python
authors = ['John Sandford', 'Emily Giffin']
df.select("author","title","rank","price").filter(df.author.isin(authors)).show()
```

<img width="836" height="511" alt="image" src="https://github.com/user-attachments/assets/e6215471-8445-4b1d-b3a6-33e12968ca26" />

## Select "author", "title" when 
* title startsWith "THE"
* title endsWith "IN"
```python
df.select("author","title").filter(col("title").startswith("THE") | col("title").endswith("IN")).show()
```

<img width="847" height="510" alt="image" src="https://github.com/user-attachments/assets/3067f6dd-3326-4b90-ae30-9e420d659711" />

## Update column 'amazon_product_url' with 'URL'
```python
df.withColumnRenamed("amazon_product_url", "URL").show()
```

<img width="2122" height="614" alt="image" src="https://github.com/user-attachments/assets/1a9f1c7f-09c4-4541-aa03-72cd54cb38bd" />

## Drop columns publisher and published_date
```python
df.drop("publisher", "published_date").show()
```

<img width="1735" height="626" alt="image" src="https://github.com/user-attachments/assets/920682f1-944c-42c9-9d43-5ed6531556da" />

## Group by author, count the books of the authors in the groups
```python
df.groupBy("author").count().show()
```

<img width="1049" height="642" alt="image" src="https://github.com/user-attachments/assets/814ac0af-3384-45a7-810a-e0908b736101" />

## Filtering entries of title Only keeps records having value 'THE HOST'
```python
df.filter(col("title") == "THE HOST").show(truncate=False)
```

<img width="2375" height="634" alt="image" src="https://github.com/user-attachments/assets/02019f6e-a075-461e-920c-5c56447ef7bd" />
<img width="917" height="565" alt="image" src="https://github.com/user-attachments/assets/ae5c44cf-8c21-406a-969d-5404cfcc5186" />
