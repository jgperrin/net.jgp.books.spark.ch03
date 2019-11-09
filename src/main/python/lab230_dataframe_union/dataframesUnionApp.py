"""
   Union of two dataframes.

   @author rambabu.posa
"""
import util
from pyspark.sql import SparkSession
import os

current_dir = os.path.dirname(__file__)
relative_path1 = "../../../../data/Restaurants_in_Wake_County_NC.csv"
absolute_file_path1 = os.path.join(current_dir, relative_path1)

relative_path2 = "../../../../data/Restaurants_in_Durham_County_NC.json"
absolute_file_path2 = os.path.join(current_dir, relative_path2)

# Creates a session on a local master
spark = SparkSession.builder.appName("Union of two dataframes") \
    .master("local[*]").getOrCreate()

df1 = spark.read.csv(path=absolute_file_path1,header=True,inferSchema=True)

df2 = spark.read.json(absolute_file_path2)


wakeRestaurantsDf = util.build_wake_restaurants_dataframe(df1)
durhamRestaurantsDf = util.build_durham_restaurants_dataframe(df2)

util.combineDataframes(wakeRestaurantsDf, durhamRestaurantsDf)


