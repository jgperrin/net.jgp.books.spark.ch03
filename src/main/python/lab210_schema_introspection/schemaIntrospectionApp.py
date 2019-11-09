"""
   Introspection of a schema.

   @author rambabu.posa
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/Restaurants_in_Wake_County_NC.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Schema introspection for restaurants in Wake County, NC") \
    .master("local[*]").getOrCreate()

# Reads a CSV file with header, called
# Restaurants_in_Wake_County_NC.csv,
# stores it in a dataframe
df = spark.read.csv(header=True, inferSchema=True,path=absolute_file_path)

# Let's transform our dataframe
df =  df.withColumn("county", F.lit("Wake")) \
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

# NEW
#//////////////////////////////////////////////////////////////////
schema = df.schema

print("*** Schema as a tree:")
df.printSchema()

# TODO: rest not working
schemaAsString = schema.mkString
print("*** Schema as string: " + schemaAsString)
schemaAsJson = schema.prettyjson
print("*** Schema as JSON: " + schemaAsJson)

# Good to stop SparkSession at the end of the application
spark.stop()
