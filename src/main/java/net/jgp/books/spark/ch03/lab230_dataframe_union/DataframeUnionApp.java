package net.jgp.books.spark.ch03.lab230_dataframe_union;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Union of two dataframes.
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
    this.spark = SparkSession.builder()
        .appName("Union of two dataframes")
        .master("local")
        .getOrCreate();

    Dataset<Row> wakeRestaurantsDf = buildWakeRestaurantsDataframe();
    Dataset<Row> durhamRestaurantsDf = buildDurhamRestaurantsDataframe();
    combineDataframes(wakeRestaurantsDf, durhamRestaurantsDf);
  }

  /**
   * Performs the union between the two dataframes.
   * 
   * @param df1
   *          Dataframe to union on
   * @param df2
   *          Dataframe to union from
   */
  private void combineDataframes(Dataset<Row> df1, Dataset<Row> df2) {
    Dataset<Row> df = df1.unionByName(df2);
    df.show(5);
    df.printSchema();
    System.out.println("We have " + df.count() + " records.");

    Partition[] partitions = df.rdd().partitions();
    int partitionCount = partitions.length;
    System.out.println("Partition count: " + partitionCount);
  }

  /**
   * Builds the dataframe containing the Wake county restaurants
   * 
   * @return A dataframe
   */
  private Dataset<Row> buildWakeRestaurantsDataframe() {
    Dataset<Row> df = this.spark.read().format("csv")
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
        .withColumn("dateEnd", lit(null))
        .withColumnRenamed("FACILITYTYPE", "type")
        .withColumnRenamed("X", "geoX")
        .withColumnRenamed("Y", "geoY")
        .drop(df.col("OBJECTID"))
        .drop(df.col("GEOCODESTATUS"))
        .drop(df.col("PERMITID"));
    df = df.withColumn("id", concat(
        df.col("state"),
        lit("_"),
        df.col("county"), lit("_"),
        df.col("datasetId")));

    // I left the following line if you want to play with repartitioning
    // df = df.repartition(4);

    return df;
  }

  /**
   * Builds the dataframe containing the Durham county restaurants
   * 
   * @return A dataframe
   */
  private Dataset<Row> buildDurhamRestaurantsDataframe() {
    Dataset<Row> df = this.spark.read().format("json")
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
        .withColumn("geoY", df.col("fields.geolocation").getItem(1))
        .drop(df.col("fields"))
        .drop(df.col("geometry"))
        .drop(df.col("record_timestamp"))
        .drop(df.col("recordid"));
    df = df.withColumn("id",
        concat(df.col("state"), lit("_"),
            df.col("county"), lit("_"),
            df.col("datasetId")));

    // I left the following line if you want to play with repartitioning
    // df = df.repartition(4);

    return df;
  }
}
