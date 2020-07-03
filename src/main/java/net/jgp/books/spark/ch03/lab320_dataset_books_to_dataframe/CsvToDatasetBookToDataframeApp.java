package net.jgp.books.spark.ch03.lab320_dataset_books_to_dataframe;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

import java.io.Serializable;
import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.jgp.books.spark.ch03.x.model.Book;

/**
 * This example will read a CSV file, ingest it in a dataframe, convert the
 * dataframe to a dataset, and vice versa.
 * 
 * @author jgp
 */
public class CsvToDatasetBookToDataframeApp implements Serializable {
  private static final long serialVersionUID = -1L;

  /**
   * This is a mapper class that will convert a Row to an instance of Book.
   * You have full control over it - isn't it great that sometimes you have
   * control?
   * 
   * @author jgp
   */
  class BookMapper implements MapFunction<Row, Book> {
    private static final long serialVersionUID = -2L;

    @Override
    public Book call(Row value) throws Exception {
      Book b = new Book();
      b.setId(value.getAs("id"));
      b.setAuthorId(value.getAs("authorId"));
      b.setLink(value.getAs("link"));
      b.setTitle(value.getAs("title"));

      // date case
      String dateAsString = value.getAs("releaseDate");
      if (dateAsString != null) {
        SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");
        b.setReleaseDate(parser.parse(dateAsString));
      }
      return b;
    }
  }

  /**
   * It starts here
   * 
   * @param args
   */
  public static void main(String[] args) {
    CsvToDatasetBookToDataframeApp app =
        new CsvToDatasetBookToDataframeApp();
    app.start();
  }

  /**
   * All the work is done here.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("CSV to dataframe to Dataset<Book> and back")
        .master("local")
        .getOrCreate();

    // Needed by Spark v3.0.0 (Thanks @dapeng09)
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");

    String filename = "data/books.csv";
    Dataset<Row> df = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(filename);

    System.out.println("*** Books ingested in a dataframe");
    df.show(5);
    df.printSchema();

    Dataset<Book> bookDs = df.map(
        new BookMapper(),
        Encoders.bean(Book.class));
    System.out.println("*** Books are now in a dataset of books");
    bookDs.show(5, 17);
    bookDs.printSchema();

    Dataset<Row> df2 = bookDs.toDF();
    df2 = df2.withColumn(
        "releaseDateAsString",
        concat(
            expr("releaseDate.year + 1900"), lit("-"),
            expr("releaseDate.month + 1"), lit("-"),
            df2.col("releaseDate.date")));
    // Although you are getting a date out this process (pretty cool, huh?),
    // this is not the recommended way to get a date. Have a look at chapter
    // 7
    // on ingestion for better ways.
    df2 = df2
        .withColumn(
            "releaseDateAsDate",
            to_date(df2.col("releaseDateAsString"), "yyyy-MM-dd"))
        .drop("releaseDateAsString");
    System.out.println("*** Books are back in a dataframe");
    df2.show(5, 13);
    df2.printSchema();
  }
}
