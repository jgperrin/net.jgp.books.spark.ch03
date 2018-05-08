package net.jgp.books.sparkWithJava.ch03.lab110JsonIngestionSchemaManipulation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.split;

/**
 * CSV ingestion in a dataframe.
 * 
 * @author jperrin
 */
public class JsonIngestionSchemaManipulationApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    JsonIngestionSchemaManipulationApp app =
        new JsonIngestionSchemaManipulationApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Restaurants in Durham County, NC")
        .master("local")
        .getOrCreate();

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    Dataset<Row> df = spark.read().format("json")
        .load("data/Restaurants_in_Durham_County_NC.json");
    df = df.withColumn("county", lit("Durham"));
    df = df.withColumn("datasetId", df.col("fields.id"));
    df = df.withColumn("name", df.col("fields.premise_name"));
    df = df.withColumn("address1", df.col("fields.premise_address1"));
    df = df.withColumn("address2", df.col("fields.premise_address2"));
    df = df.withColumn("city", df.col("fields.premise_city"));
    df = df.withColumn("state", df.col("fields.premise_state"));
    df = df.withColumn("zip", df.col("fields.premise_zip"));
    df = df.withColumn("tel", df.col("fields.premise_zip"));
    df = df.withColumn("dateStart", df.col("fields.opening_date"));
    df = df.withColumn("dateEnd", df.col("fields.closing_date"));
    df = df.withColumn("type", split(df.col("fields.type_description"), " - ").getItem(1));
    df = df.withColumn("geoX", df.col("fields.geolocation").getItem(0));
    df = df.withColumn("geoY", df.col("fields.geolocation").getItem(1));
    df = df.withColumn("id", concat(
        df.col("state"), lit("_"), df.col("county"), lit("_"),
        df.col("datasetId")));

    // Shows at most 5 rows from the dataframe
    df.show(5);
    df.printSchema();
    
  }
}
