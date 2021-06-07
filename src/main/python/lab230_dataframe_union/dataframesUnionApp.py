"""
   Union of two dataframes.

   @author rambabu.posa
"""
import os
import util
from pyspark.sql import SparkSession

def get_absolute_file_path(path, filename):
    # To get absolute path for a given filename
    current_dir = os.path.dirname(__file__)
    relative_path = "{}{}".format(path, filename)
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

def main(spark):
    # The processing code.
    filename1 = 'Restaurants_in_Wake_County_NC.csv'
    path1 = '../../../../data/'
    absolute_file_path1 = get_absolute_file_path(path1, filename1)

    filename2 = 'Restaurants_in_Durham_County_NC.json'
    path2 = '../../../../data/'
    absolute_file_path2 = get_absolute_file_path(path2, filename2)

    df1 = spark.read.csv(path=absolute_file_path1, header=True, inferSchema=True)

    df2 = spark.read.json(absolute_file_path2)

    wakeRestaurantsDf = util.build_wake_restaurants_dataframe(df1)
    durhamRestaurantsDf = util.build_durham_restaurants_dataframe(df2)

    util.combineDataframes(wakeRestaurantsDf, durhamRestaurantsDf)

if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Union of two dataframes") \
        .master("local[*]").getOrCreate()

    # Comment this line to see full log
    spark.sparkContext.setLogLevel('warn')
    main(spark)
    spark.stop()
