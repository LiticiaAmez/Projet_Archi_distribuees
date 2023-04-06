from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType

# Kafka topic to read data from
TOPIC_NAME = "meteo_data"

# Kafka broker URL and port
KAFKA_BROKER_URL = "kafka:9092"

# Spark master URL and port
SPARK_MASTER_URL = "spark://spark-master:7077"

# Create a Spark session
spark = SparkSession.builder\
    .appName("MeteoData")\
    .master(SPARK_MASTER_URL)\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# "date", "hour_data","temperature","wind"
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

# Calculate average temperature and wind per day with a 5-minute window
query = df_meteo_stream \
    .withWatermark("date", "10 minutes") \
    .groupBy(window(col("date"),"10 minutes", "5 minutes"), col("date").cast("date").alias("date")) \
    .agg({"temperature": "mean", "wind": "mean"}) \
    .select(col("window"), col("date").alias("meteo_date"), col("avg(temperature)").alias("avg_temperature"), col("avg(wind)").alias("avg_wind")) \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start() 

# Wait for a certain duration before stopping the query
query.awaitTermination() 