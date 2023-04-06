import requests
from kafka import KafkaProducer
import time
import json

# Velib API endpoint URL
API_URL = "https://api.tutiempo.net/json/?lan=fr&apid=zsTzaXzqqqXbxsh&lid=39720"
# Kafka topic to send data to
TOPIC_NAME = "meteo_data"

# Kafka broker URL and port
KAFKA_BROKER_URL = "kafka:9092"

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)

# List of columns to extract from Velib API response
columns = ["date", "hour_data","temperature","wind"]

def transform(resp):
    return [{k: resp["hour_hour"]["hour" + str(i)][k] for k in columns} for i in range(1, len(resp["hour_hour"]) + 1)]





# Function to fetch Velib data from the API
def get_meteo_data():
    response = requests.get(API_URL).json()
    if response.get("hour_hour"):
        return transform(response)[10]
    else:
        return None

# Function to send Velib data to Kafka
def send_meteo_data_to_kafka(velib_data):
    producer.send(TOPIC_NAME, json.dumps(velib_data).encode("utf-8"))

# Loop to continuously fetch and send Velib data to Kafka
while True:
    velib_data = get_meteo_data()
    if velib_data:
        send_meteo_data_to_kafka(velib_data)
    time.sleep(6) # wait for 6 seconds before sending the next message
