package net.jgp.books.spark.ch03.lab310_dataset_to_dataframe

import java.util.{Arrays, List}

import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}

/**
  * Converts an array to a Dataframe via a Dataset
  *
  * @author rambabu.posa
  */
object ArrayToDatasetToDataframeScalaApp {

  /**
    * main() is your entry point to the application.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Array to dataframe")
      .master("local").getOrCreate

    val stringList = Array[String]("Jean", "Liz", "Pierre", "Lauric")

    val data: List[String] = Arrays.asList(stringList:_*)
    /**
      * data:    parameter list1, data to create a dataset
      * encoder: parameter list2, implicit encoder
      */
    // Array to Dataset
    val ds: Dataset[String] = spark.createDataset(data)(Encoders.STRING)
    ds.show()
    ds.printSchema()

    // Dataset to Dataframe
    val df = ds.toDF
    df.show()
    df.printSchema()
  }

}
