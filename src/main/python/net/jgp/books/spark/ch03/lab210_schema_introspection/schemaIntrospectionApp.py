"""
   Introspection of a schema.

   @author rambabu.posa
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,concat

# Creates a session on a local master
spark = SparkSession.builder.appName("Schema introspection for restaurants in Wake County, NC") \
    .master("local[*]").getOrCreate()

# Reads a CSV file with header, called
# Restaurants_in_Wake_County_NC.csv,
# stores it in a dataframe
df = spark.read.csv(header=True, inferSchema=True,
                      path="../../../data/Restaurants_in_Wake_County_NC.csv")

# Let's transform our dataframe
df =  df.withColumn("county", lit("Wake")) \
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
        concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))

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

df.stop()
