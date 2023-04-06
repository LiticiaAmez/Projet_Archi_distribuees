import requests
from kafka import KafkaProducer
import time
import json

# Velib API endpoint URL
API_URL = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=500&sort=duedate&facet=name&facet=is_installed&facet=is_renting&facet=is_returning&facet=nom_arrondissement_communes&facet=duedate&refine.nom_arrondissement_communes=Paris&refine.is_installed=OUI&timezone=Europe%2FParis"

# Kafka topic to send data to
TOPIC_NAME = "velib_data"

# Kafka broker URL and port
KAFKA_BROKER_URL = "kafka:9092"

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)

# List of columns to extract from Velib API response
columns = ["duedate", "stationcode","numbikesavailable","numdocksavailable","capacity","nom_arrondissement_communes"]

# Function to extract the desired columns from Velib API response
def transform(resp):
    return [{k: resp["records"][i]["fields"][k] for k in columns} for i in range(len(resp["records"]))]

# Function to fetch Velib data from the API
def get_velib_data():
    response = requests.get(API_URL).json()
    if response.get("records"):
        return transform(response)[10]
    else:
        return None

# Function to send Velib data to Kafka
def send_velib_data_to_kafka(velib_data):
    producer.send(TOPIC_NAME, json.dumps(velib_data).encode("utf-8"))

# Loop to continuously fetch and send Velib data to Kafka
while True:
    velib_data = get_velib_data()
    if velib_data:
        send_velib_data_to_kafka(velib_data)
    time.sleep(6) # wait for 6 seconds before sending the next message
