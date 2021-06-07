"""
  util.py

  Utility component to support union of two dataframes
"""
import logging
from pyspark.sql.functions import F

"""
* Builds the dataframe containing the Wake county restaurants
*
* @return A dataframe
"""
def build_wake_restaurants_dataframe(df):
    drop_cols = ["OBJECTID", "GEOCODESTATUS", "PERMITID"]
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
        .withColumn("dateEnd", F.lit(None)) \
        .withColumnRenamed("FACILITYTYPE", "type") \
        .withColumnRenamed("X", "geoX") \
        .withColumnRenamed("Y", "geoY") \
        .drop("OBJECTID", "GEOCODESTATUS", "PERMITID")

    df = df.withColumn("id",
                       F.concat(F.col("state"), F.lit("_"),
                                F.col("county"), F.lit("_"),
                                F.col("datasetId")))
    # I left the following line if you want to play with repartitioning
    # df = df.repartition(4);
    return df

"""
* Builds the dataframe containing the Durham county restaurants
*
* @return A dataframe
"""
def build_durham_restaurants_dataframe(df):
    drop_cols = ["fields", "geometry", "record_timestamp", "recordid"]
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
            .withColumn("geoY", F.col("fields.geolocation").getItem(1)) \
            .drop(*drop_cols)

    df = df.withColumn("id",
                       F.concat(F.col("state"), F.lit("_"),
                                F.col("county"), F.lit("_"),
                                F.col("datasetId")))
    # I left the following line if you want to play with repartitioning
    # df = df.repartition(4);
    return df

"""
* Performs the union between the two dataframes.
*
* @param df1 Left Dataframe to union on
* @param df2 Right Dataframe to union from
"""
def combineDataframes(df1, df2):
    df = df1.unionByName(df2)
    df.show(5)
    df.printSchema()
    logging.warn("We have {} records.".format(df.count()))
    partition_count = df.rdd.getNumPartitions()
    logging.warn("Partition count: {}".format(partition_count))

