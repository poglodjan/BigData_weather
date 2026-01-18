import datetime
import os
import time

from kafka import KafkaConsumer

STORE = "/data"

topics = [
    "openmeteo_current",
    "openmeteo_minutely_15",
    "openmeteo_hourly",
    "electricity_mix",
    "electricity_carbon",
    "electricity_renewable",
    "electricity_load",
]

consumer = None
while consumer is None:
    try:
        print("Trying to connect to Kafka...")
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers="kafka:9092",
            group_id="weather-prediction",
            auto_offset_reset="latest",
            value_deserializer=lambda m: m.decode("utf-8", errors="ignore"),
        )
        print("Connected to Kafka!")
    except Exception as e:
        print("Kafka not ready yet, retrying in 5s...", e)
        time.sleep(5)

for msg in consumer:
    dt = datetime.datetime.now()
    dt_str = dt.isoformat()
    print(f"{dt}: Storing message from {msg.topic}")
    
    dirname = STORE + "/" + msg.topic
    os.makedirs(dirname, exist_ok=True)
    with open(dirname + "/" + dt_str, "w") as file:
        file.write(msg.value)
