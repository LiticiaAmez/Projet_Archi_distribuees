from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, date_format, sum, mean
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType
from pyspark.sql.functions import current_timestamp


# Kafka topics to read data from
VELO_TOPIC_NAME = "velib_data"
METEO_TOPIC_NAME = "meteo_data"

# Kafka broker URL and port
KAFKA_BROKER_URL = "kafka:9092"

# Spark master URL and port
SPARK_MASTER_URL = "spark://spark-master:7077"

# Create a Spark session
spark = SparkSession.builder\
    .appName("VelibMeteoJoin")\
    .master(SPARK_MASTER_URL)\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the schema for the Velib data
velib_schema = StructType([
    StructField("duedate", TimestampType()),
    StructField("stationcode", StringType()),
    StructField("numbikesavailable", IntegerType()),
    StructField("numdocksavailable", IntegerType()),
    StructField("capacity", IntegerType()),
    StructField("nom_arrondissement_communes", StringType()),
])
# Define the schema for the Meteo data
meteo_schema = StructType([
    StructField("date", TimestampType()),
    StructField("hour_data", StringType()),
    StructField("temperature", IntegerType()),
    StructField("wind", IntegerType()),
])

# Read Velib data from Kafka and save it to Spark Master
df_velib_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
    .option("subscribe", VELO_TOPIC_NAME) \
    .load() \
    .select(from_json(col("value").cast("string"), velib_schema).alias("json")) \
    .select("json.*") \
    #.withWatermark("date_V", "5 minutes")

# Read Meteo data from Kafka and save it to Spark Master
df_meteo_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
    .option("subscribe", METEO_TOPIC_NAME) \
    .load() \
    .select(from_json(col("value").cast("string"), meteo_schema).alias("json")) \
    .select("json.*")\
    .withWatermark("date", "5 minutes")


# Join Velib and Meteo data on matching dates and communes
df_join= df_velib_stream.join(df_meteo_stream, (df_meteo_stream["date"] == df_velib_stream["duedate"]) & (df_velib_stream["nom_arrondissement_communes"] == "Paris"), "inner") \
    .groupBy(window(col("date"),"10 minutes", "5 minutes"), date_format(col("date"), "yyyy-MM-dd").alias("date")) \
    .agg({"numbikesavailable": "sum", "temperature": "mean", "wind": "mean"})\
    .select(col("date"), col("sum(numbikesavailable)").alias("total_bikes_available"), col("avg(temperature)").alias("avg_temperature"), col("avg(wind)").alias("avg_wind"))

# Write the results to console
# Write the results to console with timestamp indicators
query = df_join.writeStream \
    .outputMode("Append") \
    .format("console") \
    .option("truncate", "false") \
    .start()


# Wait for the query to finish
query.awaitTermination()