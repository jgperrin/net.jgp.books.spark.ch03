package net.jgp.books.spark.ch03.lab320_dataset_books_to_dataframe

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class Book(authorId:Int, title:String, releaseDate:LocalDate, link:String, id:Int=0)

/**
  * This example will read a CSV file, ingest it in a dataframe, convert the
  * dataframe to a dataset, and vice versa.
  *
  * @author rambabu.posa
  */
object CsvToDatasetBookToDataframeScalaApp {

  /**
    * This is a mapper class that will convert a Row to an instance of Book.
    * You have full control over it - isn't it great that sometimes you have
    * control?
    *
    * @author rambabu.posa
    */
  def rowToBook(row: Row): Book = {
    val dateAsString = row.getAs[String]("releaseDate")

    val releaseDate = LocalDate.parse(
      dateAsString,
      DateTimeFormatter.ofPattern("M/d/yy")
    )

    Book(
      row.getAs[Int]("authorId"),
      row.getAs[String]("title"),
      releaseDate,
      row.getAs[String]("link"),
      row.getAs[Int]("id"))
  }

  /**
    * main() is your entry point to the application.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
                            .appName("CSV to dataframe to Dataset<Book> and back")
                            .master("local")
                            .getOrCreate


    val filename = "data/books.csv"
    val df = spark.read.format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filename)

    println("*** Books ingested in a dataframe")
    df.show(5)
    df.printSchema()

    import spark.implicits._
    val bookDs:Dataset[Book] = df.map(rowToBook)

    println("*** Books are now in a dataset of books")
    bookDs.show(5, 17)
    bookDs.printSchema()

    var df2 = bookDs.toDF

    df2 = df2.withColumn("releaseDateAsString",
      F.date_format(F.col("releaseDate"), "M/d/yy").as("MM/dd/yyyy"))

    println("*** Books are back in a dataframe")
    df2.show(5, 13)
    df2.printSchema()

  }

}
