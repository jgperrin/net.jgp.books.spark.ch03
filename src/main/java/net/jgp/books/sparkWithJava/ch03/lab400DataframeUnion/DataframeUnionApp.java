package net.jgp.books.sparkWithJava.ch03.lab400DataframeUnion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.Partition;

/**
 * CSV ingestion in a dataframe.
 * 
 * @author jgp
 */
public class DataframeUnionApp {
  private SparkSession spark;

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    DataframeUnionApp app =
        new DataframeUnionApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    spark = SparkSession.builder()
        .appName("Restaurants in Durham County, NC")
        .master("local")
        .getOrCreate();

    Dataset<Row> wakeRestaurantsDf = buildWakeRestaurantsDataframe();
    Dataset<Row> durhamRestaurantsDf = buildDurhamRestaurantsDataframe();
    
    Dataset<Row> df = wakeRestaurantsDf.union(durhamRestaurantsDf);
    df.show(5);
    df.printSchema();
    System.out.println("We have " + df.count() + " records.");
  }

  /**
   * Builds the dataframe containing the Wake county restaurants
   * @return
   */
  private Dataset<Row> buildWakeRestaurantsDataframe() {
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .load("data/Restaurants_in_Wake_County_NC.csv");
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
    return df;
  }

  /**
   * Builds the dataframe containing the Durham county restaurants
   * @return
   */
  private Dataset<Row> buildDurhamRestaurantsDataframe() {
    Dataset<Row> df = spark.read().format("json")
        .load("data/Restaurants_in_Durham_County_NC.json");
    df = df.withColumn("county", lit("Durham"))
        .withColumn("datasetId", df.col("fields.id"))
        .withColumn("name", df.col("fields.premise_name"))
        .withColumn("address1", df.col("fields.premise_address1"))
        .withColumn("address2", df.col("fields.premise_address2"))
        .withColumn("city", df.col("fields.premise_city"))
        .withColumn("state", df.col("fields.premise_state"))
        .withColumn("zip", df.col("fields.premise_zip"))
        .withColumn("tel", df.col("fields.premise_phone"))
        .withColumn("dateStart", df.col("fields.opening_date"))
        .withColumn("dateEnd", df.col("fields.closing_date"))
        .withColumn("type",
            split(df.col("fields.type_description"), " - ").getItem(1))
        .withColumn("geoX", df.col("fields.geolocation").getItem(0))
        .withColumn("geoY", df.col("fields.geolocation").getItem(1));
    df = df.withColumn("id",
        concat(df.col("state"), lit("_"),
            df.col("county"), lit("_"),
            df.col("datasetId")));
    return df;
  }
}
