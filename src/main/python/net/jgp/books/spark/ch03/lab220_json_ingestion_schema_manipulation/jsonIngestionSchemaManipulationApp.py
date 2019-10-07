"""
   CSV ingestion in a dataframe.

   @author rambabu.posa
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,concat

# Creates a session on a local master
spark = SparkSession.builder.appName("Restaurants in Durham County, NC") \
    .master("local[*]").getOrCreate()

# Reads a JSON file called Restaurants_in_Durham_County_NC.json, stores
# it
# in a dataframe
df = spark.read.format("json").load("data/Restaurants_in_Durham_County_NC.json")
print("*** Right after ingestion")
df.show(5)
df.printSchema()
print("We have " + df.count + " records.")

