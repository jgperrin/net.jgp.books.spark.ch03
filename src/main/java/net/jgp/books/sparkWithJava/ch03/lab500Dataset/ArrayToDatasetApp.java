package net.jgp.books.sparkWithJava.ch03.lab500Dataset;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 * Converts an array to a Dataset
 * 
 * @author jperrin
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

    String[] l = new String[] { "a", "b", "c", "d" };
    List<String> data = Arrays.asList(l);
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    ds.show();
    ds.printSchema();
  }
}
