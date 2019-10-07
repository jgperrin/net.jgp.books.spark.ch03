package net.jgp.books.spark.ch03.lab320_dataset_books_to_dataframe

import java.text.SimpleDateFormat
import java.util.Date
import net.jgp.books.spark.ch03.y.model.Book
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.functions.{col, concat, expr, lit, to_date}
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}

/**
  * This example will read a CSV file, ingest it in a dataframe, convert the
  * dataframe to a dataset, and vice versa.
  *
  * @author rambabu.posa
  */
object Csv2DatasetBook2DataframeApp {

  /**
    * This is a mapper class that will convert a Row to an instance of Book.
    * You have full control over it - isn't it great that sometimes you have
    * control?
    *
    * @author jgp
    */
  @SerialVersionUID(-2L)
  private[lab320_dataset_books_to_dataframe] class BookMapper extends MapFunction[Row, Book] {
    @throws[Exception]
    override def call(value: Row): Book = {

      val dateAsString = value.getAs("releaseDate")

      // date case
      val date = if( dateAsString != null) {
          val parser = new SimpleDateFormat("M/d/yy")
          parser.parse(dateAsString)
      } else {
        null
      }

      Book(value.getAs("authorId"),
        value.getAs("link"),
        date,
        value.getAs("title")
      )
    }
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

    val bookDs = df.map(new BookMapper, Encoders.bean(classOf[Book]))
    println("*** Books are now in a dataset of books")
    bookDs.show(5, 17)
    bookDs.printSchema()

    var df2 = bookDs.toDF
    val cols_list = List(expr("releaseDate.year + 1900"),
                        lit("-"),
                        expr("releaseDate.month + 1"),
                        lit("-"),
                        col("releaseDate.date"))

    df2 = df2.withColumn("releaseDateAsString", concat(cols_list:_*))
    // Although you are getting a date out this process (pretty cool, huh?),
    // this is not the recommended way to get a date. Have a look at chapter 7
    // on ingestion for better ways.


    df2 = df2.withColumn("releaseDateAsDate", to_date(col("releaseDateAsString"), "yyyy-MM-dd"))
             .drop("releaseDateAsString")
    println("*** Books are back in a dataframe")
    df2.show(5, 13)
    df2.printSchema()
  }

}
