package net.jgp.books.spark.ch03.lab210_schema_introspection

import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SparkSession

/**
  * Introspection of a schema.
  *
  * @author rambabu.posa
  */
object SchemaIntrospectionScalaApp {
  /**
    * main() is your entry point to the application.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // Similar to IngestionSchemaManipulationApp (no output)
    ////////////////////////////////////////////////////////////////////
    // Creates a session on a local master
    val spark = SparkSession.builder.appName("Schema introspection for restaurants in Wake County, NC")
      .master("local").getOrCreate

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    var df = spark.read.format("csv").option("header", "true")
      .load("data/Restaurants_in_Wake_County_NC.csv")

    // Let's transform our dataframe
    df = df.withColumn("county", lit("Wake"))
          .withColumnRenamed("HSISID", "datasetId")
          .withColumnRenamed("NAME", "name")
          .withColumnRenamed("ADDRESS1", "address1")
          .withColumnRenamed("ADDRESS2", "address2")
          .withColumnRenamed("CITY", "city")
          .withColumnRenamed("STATE", "state")
          .withColumnRenamed("POSTALCODE", "zip")
          .withColumnRenamed("PHONENUMBER", "tel")
          .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
          .withColumnRenamed("FACILITYTYPE", "type")
          .withColumnRenamed("X", "geoX")
          .withColumnRenamed("Y", "geoY")

    df = df.withColumn("id",
      concat(df.col("state"), lit("_"), df.col("county"), lit("_"), df.col("datasetId")))

    // NEW
    ////////////////////////////////////////////////////////////////////
    val schema = df.schema

    println("*** Schema as a tree:")
    schema.printTreeString()
    val schemaAsString = schema.mkString
    println("*** Schema as string: " + schemaAsString)
    val schemaAsJson = schema.prettyJson
    println("*** Schema as JSON: " + schemaAsJson)
  }

}
