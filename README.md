The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.net/sia).

# Spark in Action, 2nd edition - chapter 3

Welcome to Spark with Java, chapter 3. This chapter is all about the dataframe, understand its role and royalty!

Labs:
 * #200: `IngestionSchemaManipulationApp`: ingestion of a CSV, manipulation of schema structure post-ingestion

Notes: 
 1. Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10.

---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://www.facebook.com/SparkWithJava/) or in [Manning's community site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).

It explains how to run Pyspark/Python application

1. Clone this project
   Assume that cloned this project to ${MY_HOME_DIR}

2. cd ${MY_HOME_DIR}/src/main/python

3. Execute the following spark-submit command to create a jar file to our this application
```
spark-submit net/jgp/books/spark/ch03/lab200_ingestion_schema_manipulation/ingestionSchemaManipulationApp.py
```

It explains how to run Spark/Scala application

1. Clone this project
   Assume that cloned this project to ${MY_HOME_DIR}

2. cd ${MY_HOME_DIR}/src/main/scala

3. Execute the following spark-submit command to create a jar file to our this application
```
spark-submit net.jgp.books.spark.ch03.lab200_ingestion_schema_manipulation.ingestionSchemaManipulateApp
```