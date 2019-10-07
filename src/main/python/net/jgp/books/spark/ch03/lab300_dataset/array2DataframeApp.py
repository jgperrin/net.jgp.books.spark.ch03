"""
  Converts an array to a Dataframe of strings.

   @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType

# Creates a session on a local master
spark = SparkSession.builder.appName("Array to Dataframe") \
    .master("local[*]").getOrCreate()

data = [['Jean'], ['Liz'], ['Pierre'], ['Lauric']]

"""
* data:    parameter list1, data to create a dataset
* encoder: parameter list2, implicit encoder
"""
schema = StructType([StructField('name', StringType(), True)])

df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

