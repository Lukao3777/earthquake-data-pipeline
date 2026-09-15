import json
import requests
from confluent_kafka import Producer

USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(KAFKA_CONFIG)

def fetch_earthquakes():
    response = requests.get(USGS_URL, timeout=10)
    response.raise_for_status()

    data = response.json()
    return data["features"]

def transform_earthquake(earthquake):
    properties = earthquake["properties"]
    coordinates = earthquake["geometry"]["coordinates"]

    return {
        "event_id": earthquake["id"],
        "magnitude": properties["mag"],
        "place": properties["place"],
        "longitude": coordinates[0],
        "latitude": coordinates[1],
        "depth_km": coordinates[2],
        "event_time": properties["time"],
    }

def send_to_kafka(earthquake):
    message = json.dumps(earthquake)

    producer.produce(
        topic="earthquakes",
        value=message
    )

if __name__ == "__main__":
    earthquakes = fetch_earthquakes()

    print(f"Earthquakes found: {len(earthquakes)}")

    for earthquake in earthquakes:
        transformed = transform_earthquake(earthquake)
        send_to_kafka(transformed)
        print(transformed)

    producer.flush()