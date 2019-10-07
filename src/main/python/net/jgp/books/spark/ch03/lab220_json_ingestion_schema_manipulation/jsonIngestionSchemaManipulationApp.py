"""
   CSV ingestion in a dataframe.

   @author rambabu.posa
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (lit,col,concat,split)

# Creates a session on a local master
spark = SparkSession.builder.appName("Restaurants in Durham County, NC") \
    .master("local[*]").getOrCreate()

# Reads a JSON file called Restaurants_in_Durham_County_NC.json, stores
# it in a dataframe
df = spark.read.json("../../../data/Restaurants_in_Durham_County_NC.json")
print("*** Right after ingestion")
df.show(5)
df.printSchema()
print("We have {} records.".format(df.count))

df =  df.withColumn("county", lit("Durham")) \
        .withColumn("datasetId", col("fields.id")) \
        .withColumn("name", col("fields.premise_name")) \
        .withColumn("address1", col("fields.premise_address1")) \
        .withColumn("address2", col("fields.premise_address2")) \
        .withColumn("city", col("fields.premise_city")) \
        .withColumn("state", col("fields.premise_state")) \
        .withColumn("zip", col("fields.premise_zip")) \
        .withColumn("tel", col("fields.premise_phone")) \
        .withColumn("dateStart", col("fields.opening_date")) \
        .withColumn("dateEnd", col("fields.closing_date")) \
        .withColumn("type", split(col("fields.type_description"), " - ").getItem(1)) \
        .withColumn("geoX", col("fields.geolocation").getItem(0)) \
        .withColumn("geoY", col("fields.geolocation").getItem(1))

df = df.withColumn("id", concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))

print("*** Dataframe transformed")
df.select('id',"state", "county", "datasetId").show(5)
df.printSchema()

print("*** Looking at partitions")
partitionCount = df.rdd.getNumPartitions()
print("Partition count before repartition: {}".format(partitionCount))

df = df.repartition(4)
print("Partition count after repartition: {}".format(df.rdd.getNumPartitions()))