from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, window,to_timestamp
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType


# Kafka topic to read data from
TOPIC_NAME = "velib_data"

# Kafka broker URL and port
KAFKA_BROKER_URL = "kafka:9092"

# Spark master URL and port
SPARK_MASTER_URL = "spark://9099242b61f7:7077"


# Create a Spark session
spark = SparkSession.builder\
    .appName("VelibData")\
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

# Read data from Kafka and save it to Spark Master
df_velib_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
    .option("subscribe", TOPIC_NAME) \
    .load() \
    .select(from_json(col("value").cast("string"), velib_schema).alias("json")) \
    .select("json.*") \
    .withWatermark("duedate", "10 minutes")



# Filter the data by the commune of Paris
df_paris = df_velib_stream.filter(col("nom_arrondissement_communes") == "Paris")

# Aggregate by date and count the number of bikes available
df_count_by_date = df_paris.groupBy(
    window("duedate","10 minutes", "5 minutes"),
    date_format("duedate", "yyyy-MM-dd").alias("date")
).sum("numbikesavailable").withColumnRenamed("sum(numbikesavailable)", "num_bikes_available")




query = df_count_by_date.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .outputMode("complete")\
    .start()

query.awaitTermination()





















































































