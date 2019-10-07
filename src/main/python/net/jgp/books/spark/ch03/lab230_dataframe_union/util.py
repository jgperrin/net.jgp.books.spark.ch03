"""
  util.py

  Utility component to support union of two dataframes
"""

from pyspark.sql.functions import (lit,col,concat,split)

"""
* Builds the dataframe containing the Wake county restaurants
*
* @return A dataframe
"""
def build_wake_restaurants_dataframe(df):
    drop_cols = ["OBJECTID", "GEOCODESTATUS", "PERMITID"]
    df = df.withColumn("county", lit("Wake")) \
        .withColumnRenamed("HSISID", "datasetId") \
        .withColumnRenamed("NAME", "name") \
        .withColumnRenamed("ADDRESS1", "address1") \
        .withColumnRenamed("ADDRESS2", "address2") \
        .withColumnRenamed("CITY", "city") \
        .withColumnRenamed("STATE", "state") \
        .withColumnRenamed("POSTALCODE", "zip") \
        .withColumnRenamed("PHONENUMBER", "tel") \
        .withColumnRenamed("RESTAURANTOPENDATE", "dateStart") \
        .withColumn("dateEnd", lit(None)) \
        .withColumnRenamed("FACILITYTYPE", "type") \
        .withColumnRenamed("X", "geoX") \
        .withColumnRenamed("Y", "geoY") \
        .drop("OBJECTID", "GEOCODESTATUS", "PERMITID")

    df = df.withColumn("id",
                       concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))
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
            .withColumn("geoY", col("fields.geolocation").getItem(1)) \
            .drop(*drop_cols)

    df = df.withColumn("id",
                       concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))
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
    print("We have {} records.".format(df.count()))
    partition_count = df.rdd.getNumPartitions()
    print("Partition count: {}".format(partition_count))

