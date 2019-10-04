"""
  CSV ingestion in a dataframe and manipulation.

  @author rambabu.posa
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,concat

# Creates a session on a local master
session = SparkSession.builder.appName("Restaurants in Wake County, NC") \
    .master("local[*]").getOrCreate()


# Reads a CSV file with header, called
# Restaurants_in_Wake_County_NC.csv,
# stores it in a dataframe
df = session.read.csv(header=True, inferSchema=True, path="../../../data/Restaurants_in_Wake_County_NC.csv")

print("*** Right after ingestion")
df.show(5)
df.printSchema()

print("We have {} records.".format(df.count()))

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
        .withColumnRenamed("Y", "geoY") \
        .drop("OBJECTID", "PERMITID", "GEOCODESTATUS")

df = df.withColumn("id",
        concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))

# Shows at most 5 rows from the dataframe
print("*** Dataframe transformed")
df.show(5)


# for book only
drop_cols=["address2","zip","tel","dateStart",
           "geoX","geoY","address1","datasetId"]
dfUsedForBook  =  df.drop(drop_cols)

dfUsedForBook.show(5, 15)
# end

df.printSchema()

print("*** Looking at partitions")
partitionCount = df.rdd.getNumPartitions()
print("Partition count before repartition: {}".format(partitionCount))

df = df.repartition(4)

print("Partition count after repartition: {}".format(df.rdd.getNumPartitions()))