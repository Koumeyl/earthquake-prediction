from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Set localhost socket parameters from ther server
localhost = "127.0.0.1"
local_port = 9095

# Create Spark session
spark = SparkSession.builder.appName("Twitter Stream Reader") \
.config("spark.jars.packages", "org.postgresql:postgresql:42.4.0") \
.getOrCreate()

# Create streaming DataFrame from local socket
# delimiter added on server side
lines = spark.readStream.format("socket") \
    .option("host", localhost) \
    .option("port", local_port) \
    .option("delimiter", "\n") \
    .option("includeTimestamp", True) \
    .load()

# Create df from raw stream data
df = lines.select(json_tuple(col("value"),"id","text","lang","author_id"),"timestamp") \
    .toDF("id","text","lang","author_id","timestamp")

# Define the function to write the postgre data
def patch_postgre (df , batchID) :
    df.write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/twitterdb") \
    .option("dbtable", "tbtweets_user") \
    .option("user", "twitteru1") \
    .option("password","1234").option("driver","org.postgresql.Driver") \
    .option("truncate", False) \
    .mode("append").save()


# Send stream data into postgresql table
query =  df.writeStream.foreachBatch(patch_postgre).outputMode("append") \
    .trigger(processingTime = "2 second").option("checkpointlocation" , "checkpoint/").start().awaitTermination()


