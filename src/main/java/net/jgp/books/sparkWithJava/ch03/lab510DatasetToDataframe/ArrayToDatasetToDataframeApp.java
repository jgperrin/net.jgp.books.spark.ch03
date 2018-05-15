package net.jgp.books.sparkWithJava.ch03.lab510DatasetToDataframe;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Converts an array to a Dataset
 * 
 * @author jperrin
 */
public class ArrayToDatasetToDataframeApp {

  public static void main(String[] args) {
    ArrayToDatasetToDataframeApp app = new ArrayToDatasetToDataframeApp();
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

    Dataset<Row> df = ds.toDF();
    df.show();
    df.printSchema();
  }
}
