"""
   CSV ingestion in a dataframe.

   @author rambabu.posa
"""
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import F

def get_absolute_file_path(path, filename):
        # To get absolute path for a given filename
        current_dir = os.path.dirname(__file__)
        relative_path = "{}{}".format(path, filename)
        absolute_file_path = os.path.join(current_dir, relative_path)
        return absolute_file_path

def main(spark):
        # The processing code.
        filename = 'Restaurants_in_Durham_County_NC.json'
        path = '../../../../data/'
        absolute_file_path = get_absolute_file_path(path, filename)

        # Reads a JSON file called Restaurants_in_Durham_County_NC.json, stores
        # it in a dataframe
        df = spark.read.json(absolute_file_path)
        logging.warn("*** Right after ingestion")
        df.show(5)
        df.printSchema()
        logging.warn("We have {} records.".format(df.count))

        df =  df.withColumn("county", F.lit("Durham")) \
                .withColumn("datasetId", F.col("fields.id")) \
                .withColumn("name", F.col("fields.premise_name")) \
                .withColumn("address1", F.col("fields.premise_address1")) \
                .withColumn("address2", F.col("fields.premise_address2")) \
                .withColumn("city", F.col("fields.premise_city")) \
                .withColumn("state", F.col("fields.premise_state")) \
                .withColumn("zip", F.col("fields.premise_zip")) \
                .withColumn("tel", F.col("fields.premise_phone")) \
                .withColumn("dateStart", F.col("fields.opening_date")) \
                .withColumn("dateEnd", F.col("fields.closing_date")) \
                .withColumn("type", F.split(F.col("fields.type_description"), " - ").getItem(1)) \
                .withColumn("geoX", F.col("fields.geolocation").getItem(0)) \
                .withColumn("geoY", F.col("fields.geolocation").getItem(1))

        df = df.withColumn("id", F.concat(F.col("state"), F.lit("_"),
                                          F.col("county"), F.lit("_"),
                                          F.col("datasetId")))

        logging.warn("*** Dataframe transformed")
        df.select('id',"state", "county", "datasetId").show(5)
        df.printSchema()

        logging.warn("*** Looking at partitions")
        partitionCount = df.rdd.getNumPartitions()
        logging.warn("Partition count before repartition: {}".format(partitionCount))

        df = df.repartition(4)
        logging.warn("Partition count after repartition: {}".format(df.rdd.getNumPartitions()))

if __name__ == '__main__':
        # Creates a session on a local master
        spark = SparkSession.builder.appName("Restaurants in Durham County, NC") \
                .master("local[*]").getOrCreate()

        # Comment this line to see full log
        spark.sparkContext.setLogLevel('warn')
        main(spark)
        spark.stop()