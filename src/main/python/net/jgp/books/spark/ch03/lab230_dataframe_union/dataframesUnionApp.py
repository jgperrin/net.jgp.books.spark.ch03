"""
   Union of two dataframes.

   @author rambabu.posa
"""
import util
from pyspark.sql import SparkSession

# Creates a session on a local master
spark = SparkSession.builder.appName("Union of two dataframes") \
    .master("local[*]").getOrCreate()

df1 = spark.read.csv(path="../../../data/Restaurants_in_Wake_County_NC.csv",header=True)

df2 = spark.read.json("../../../data/Restaurants_in_Durham_County_NC.json")


wakeRestaurantsDf = util.build_wake_restaurants_dataframe(df1)
durhamRestaurantsDf = util.build_durham_restaurants_dataframe(df2)

util.combineDataframes(wakeRestaurantsDf, durhamRestaurantsDf)


