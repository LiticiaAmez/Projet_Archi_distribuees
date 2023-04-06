import pandas as pd
from pyspark.sql.functions import from_json, col, window, udf, date_format
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType
from pymongo import MongoClient
from datetime import datetime
from pyspark.sql import SparkSession

# Kafka topic to read data from
TOPIC_NAME = "meteo_data"

# Kafka broker URL and port
KAFKA_BROKER_URL = "kafka:9092"

# Spark master URL and port
SPARK_MASTER_URL = "spark://9099242b61f7:7077"

# MongoDB connection details
MONGODB_CONNECTION_STRING = "mongodb+srv://liticia:kafka123@bddkafka.oekdlra.mongodb.net/?retryWrites=true&w=majority"
MONGODB_DATABASE_NAME = "mydatabase"
MONGODB_COLLECTION_NAME = "collection_meteo"

# Output directory for json files
JSON_OUTPUT_DIR = "data_meteo"

# Create a Spark session
spark = SparkSession.builder\
    .appName("MeteoData")\
    .master(SPARK_MASTER_URL)\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the schema for the Meteo data
meteo_schema = StructType([
    StructField("date", TimestampType()),
    StructField("hour_data", StringType()),
    StructField("temperature", IntegerType()),
    StructField("wind", IntegerType()),
])

# Read data from Kafka and save it to Spark Master
df_meteo_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
    .option("subscribe", TOPIC_NAME) \
    .load() \
    .select(from_json(col("value").cast("string"), meteo_schema).alias("json")) \
    .select("json.*")

# Define a UDF to convert timestamp to date
@udf
def convert_to_date(timestamp):
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")

# Calculate average temperature and wind per day with a 5-minute window
query = df_meteo_stream \
    .withWatermark("date", "5 minutes") \
    .withColumn("date_only", convert_to_date(col("date").cast("long"))) \
    .groupBy(window(col("date"), "10 minutes", "5 minutes"), col("date_only")) \
    .agg({"temperature": "mean", "wind": "mean"}) \
    .select(col("window"), col("date_only"), col("avg(temperature)").alias("avg_temperature"), col("avg(wind)").alias("avg_wind"), date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"), date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"))

# Define the MongoDB client
client = MongoClient(MONGODB_CONNECTION_STRING)
db = client[MONGODB_DATABASE_NAME]
collection = db[MONGODB_COLLECTION_NAME]

# Start the streaming query
query.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda batch, _: collection.insert_many(batch.toPandas().to_dict("records")) if batch.count() > 0 else None) \
    .start() \
    .awaitTermination()