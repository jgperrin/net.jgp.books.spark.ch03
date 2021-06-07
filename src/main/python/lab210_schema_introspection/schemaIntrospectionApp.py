"""
   Introspection of a schema.

   @author rambabu.posa
"""
import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_absolute_file_path(path, filename):
        # To get absolute path for a given filename
        current_dir = os.path.dirname(__file__)
        relative_path = "{}{}".format(path, filename)
        absolute_file_path = os.path.join(current_dir, relative_path)
        return absolute_file_path

def main(spark):
        # The processing code.
        filename = 'Restaurants_in_Wake_County_NC.csv'
        path = '../../../../data/'
        absolute_file_path = get_absolute_file_path(path, filename)
        # Reads a CSV file with header, called
        # Restaurants_in_Wake_County_NC.csv,
        # stores it in a dataframe
        df = spark.read.csv(header=True, inferSchema=True, path=absolute_file_path)

        # Let's transform our dataframe
        df = df.withColumn("county", F.lit("Wake")) \
                .withColumnRenamed("HSISID", "datasetId") \
                .withColumnRenamed("NAME", "name") \
                .withColumnRenamed("ADDRESS1", "address1") \
                .withColumnRenamed("ADDRESS2", "address2") \
                .withColumnRenamed("CITY", "city") \
                .withColumnRenamed("STATE", "state") \
                .withColumnRenamed("POSTALCODE", "zip") \
                .withColumnRenamed("PHONENUMBER", "tel") \
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart") \
                .withColumnRenamed("FACILITYTYPE", "type") \
                .withColumnRenamed("X", "geoX") \
                .withColumnRenamed("Y", "geoY")

        df = df.withColumn("id",
                F.concat(F.col("state"), F.lit("_"), F.col("county"), F.lit("_"), F.col("datasetId")))

        logging.warn("*** Schema as a tree:")
        df.printSchema()

        logging.warn("*** Schema as string: {}".format(df.schema))
        schemaAsJson = df.schema.json()
        parsedSchemaAsJson = json.loads(schemaAsJson)

        logging.warn("*** Schema as JSON: {}".format(json.dumps(parsedSchemaAsJson, indent=2)))

if __name__ == '__main__':
        # Creates a session on a local master
        spark = SparkSession.builder \
                .appName("Schema introspection for restaurants in Wake County, NC") \
                .master("local[*]").getOrCreate()

        # Comment this line to see full log
        spark.sparkContext.setLogLevel('warn')
        main(spark)
        # Good to stop SparkSession at the end of the application
        spark.stop()
