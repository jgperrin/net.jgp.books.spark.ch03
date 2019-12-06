package net.jgp.books.spark.ch03.lab220_json_ingestion_schema_manipulation

import org.apache.spark.sql.functions.{col, concat, lit, split}
import org.apache.spark.sql.SparkSession

/**
  * CSV ingestion in a dataframe.
  *
  * @author rambabu.posa
  */
object JsonIngestionSchemaManipulationScalaApp {

  /**
    * main() is your entry point to the application.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // Creates a session on a local master
    val spark = SparkSession.builder.appName("Restaurants in Durham County, NC")
      .master("local[*]").getOrCreate

    // Reads a JSON file called Restaurants_in_Durham_County_NC.json, stores
    // it
    // in a dataframe
    var df = spark.read.format("json").load("data/Restaurants_in_Durham_County_NC.json")
    println("*** Right after ingestion")
    df.show(5)
    df.printSchema()
    println("We have " + df.count + " records.")

    df = df.withColumn("county", lit("Durham"))
          .withColumn("datasetId", col("fields.id"))
          .withColumn("name", col("fields.premise_name"))
          .withColumn("address1", col("fields.premise_address1"))
          .withColumn("address2", col("fields.premise_address2"))
          .withColumn("city", col("fields.premise_city"))
          .withColumn("state", col("fields.premise_state"))
          .withColumn("zip", col("fields.premise_zip"))
          .withColumn("tel", col("fields.premise_phone"))
          .withColumn("dateStart", col("fields.opening_date"))
          .withColumn("dateEnd", col("fields.closing_date"))
          .withColumn("type", split(col("fields.type_description"), " - ").getItem(1))
          .withColumn("geoX", col("fields.geolocation").getItem(0))
          .withColumn("geoY", col("fields.geolocation").getItem(1))

    val cols_list = List(col("state"), lit("_"), col("county"), lit("_"), col("datasetId"))

    df = df.withColumn("id", concat(cols_list:_*))

    println("*** Dataframe transformed")
    df.show(5)
    df.printSchema()

    println("*** Looking at partitions")
    val partitionCount = df.rdd.getNumPartitions
    println("Partition count before repartition: " + partitionCount)

    df = df.repartition(4)
    println("Partition count after repartition: " + df.rdd.getNumPartitions)
  }

}
