package net.jgp.books.spark.ch03.lab300_dataset;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 * Converts an array to a Dataset of strings
 * 
 * @author jgp
 */
public class ArrayToDatasetApp {

  public static void main(String[] args) {
    ArrayToDatasetApp app = new ArrayToDatasetApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Array to Dataset<String>")
        .master("local")
        .getOrCreate();

    String[] stringList =
        new String[] { "Jean", "Liz", "Pierre", "Lauric" };
    List<String> data = Arrays.asList(stringList);
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    ds.show();
    ds.printSchema();
  }
}
