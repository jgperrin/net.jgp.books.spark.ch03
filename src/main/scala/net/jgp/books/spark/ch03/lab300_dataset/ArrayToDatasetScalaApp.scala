package net.jgp.books.spark.ch03.lab300_dataset

import java.util.{Arrays, List}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

/**
  * Converts an array to a Dataset of strings
  *
  * @author rambabu.posa
  */
object ArrayToDatasetScalaApp {

  /**
    * main() is your entry point to the application.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("Array to Dataset<String>")
                .master("local").getOrCreate

    val stringList: Array[String] = Array[String]("Jean", "Liz", "Pierre", "Lauric")
    val data:List[String] = Arrays.asList(stringList:_*)
    /**
      * data:    parameter list1, data to create a dataset
      * encoder: parameter list2, implicit encoder
      */
    val ds: Dataset[String] = spark.createDataset(data)(Encoders.STRING)

    ds.show()
    ds.printSchema()

  }

}
