The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean-Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.net/sia).

# Spark in Action, 2nd edition - chapter 3

Welcome to Spark in Action, 2nd edition, chapter 3. This chapter is all about the dataframe, understand its role and royalty!

This code is designed to work with Apache Spark v3.1.2.

## Labs

Each chapter has one or more labs. Labs are examples used for teaching in the [book](https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp). You are encouraged to take ownership of the code and modify it, experiment with it, hence the use of the term **lab**.

### Lab \#200

`IngestionSchemaManipulationApp`: ingestion of a CSV file, manipulation of schema structure post-ingestion

## Running the labs

### Running the labs in Java

For information on running the Java lab, see chapter 3 in [Spark in Action, 2nd edition](http://jgp.net/sia).


### Running the labs using PySpark

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').

1. Clone this project

    git clone https://github.com/jgperrin/net.jgp.books.spark.ch03

2. Go to the lab in the Python directory

Here, we show lab #200.

    cd net.jgp.books.spark.ch03/src/main/python/lab200_ingestion_schema_manipulation/

3. Execute the following spark-submit command to create a jar file to our this application

    spark-submit ingestionSchemaManipulationApp.py

### Running the lab in Scala

You will need:

 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips'). 

1. Clone this project

    git clone https://github.com/jgperrin/net.jgp.books.spark.ch03

2. Go to the directory

    cd net.jgp.books.spark.ch03

3. Package application using sbt command

    sbt clean assembly

4. Run Spark/Scala application using spark-submit command as shown below:

    spark-submit --class net.jgp.books.spark.ch03.lab200_ingestion_schema_manipulation.IngestionSchemaManipulationScalaApp target/scala-2.12/SparkInAction2-Chapter03-assembly-1.0.0.jar

## News
 1. [2020-06-07] Updated the pom.xml to support Apache Spark v3.1.2. 
 1. [2020-06-07] As we celebrate the first anniversary of Spark in Action, 2nd edition is the best-rated Apache Spark book on [Amazon](https://amzn.to/2TPnmOv). 

## Notes: 
 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 1. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 1. The master branch contains the last version of the code running against the latest supported version of Apache Spark. Look in specifics branches for specific versions.
   
---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://fb.com/SparkInAction/) or in [Manning's community site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).
