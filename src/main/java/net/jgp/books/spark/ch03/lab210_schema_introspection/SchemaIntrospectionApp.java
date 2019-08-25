package net.jgp.books.spark.ch03.lab210_schema_introspection;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Introspection of a schema.
 * 
 * @author jgp
 */
public class SchemaIntrospectionApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    SchemaIntrospectionApp app = new SchemaIntrospectionApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {

    // Similar to IngestionSchemaManipulationApp (no output)
    ////////////////////////////////////////////////////////////////////

    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Schema introspection for restaurants in Wake County, NC")
        .master("local")
        .getOrCreate();

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .load("data/Restaurants_in_Wake_County_NC.csv");

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
        .withColumnRenamed("Y", "geoY");
    df = df.withColumn("id", concat(
        df.col("state"),
        lit("_"),
        df.col("county"), lit("_"),
        df.col("datasetId")));

    // NEW
    ////////////////////////////////////////////////////////////////////

    StructType schema = df.schema();

    System.out.println("*** Schema as a tree:");
    schema.printTreeString();
    String schemaAsString = schema.mkString();
    System.out.println("*** Schema as string: " + schemaAsString);
    String schemaAsJson = schema.prettyJson();
    System.out.println("*** Schema as JSON: " + schemaAsJson);
  }
}
