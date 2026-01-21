#!/usr/bin/env python3

import datetime
import os
import sys
import time

from kafka import KafkaProducer

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

# Replay messages this many times faster
SPEEDUP = 30
start_dt = None
if len(sys.argv) >= 2:
    start_dt = datetime.datetime.fromisoformat(sys.argv[1])
if len(sys.argv) >= 3:
    SPEEDUP = int(sys.argv[2])

producer = None
while producer is None:
    try:
        print("Trying to connect to Kafka...")
        producer = KafkaProducer(bootstrap_servers="kafka:9092")
        print("Connected to Kafka!")
    except Exception as e:
        print("Kafka not ready yet, retrying in 5s...", e)
        time.sleep(5)

def list_available():
    messages = []
    for topic in topics:
        dirname = STORE + "/" + topic
        if os.path.isdir(dirname):
            files = os.listdir(dirname)
            for fname in files:
                try:
                    dt = datetime.datetime.fromisoformat(fname)
                except ValueError:
                    continue
                if start_dt is None or dt >= start_dt:
                    path = dirname + "/" + fname
                    messages.append((dt, topic, path))
    messages.sort()
    return messages

messages = list_available()

print(f"Found {len(messages)} messages to replay")

if len(messages) == 0:
    exit(2)

cur_time = messages[0][0]

for (dt, topic, path) in messages:
    delay = dt - cur_time
    wait = delay.total_seconds() / SPEEDUP
    time.sleep(wait)
    new_topic = topic + "_out"
    with open(path, "rb") as file:
        producer.send(new_topic, file.read())
    print(dt, new_topic)
    cur_time = dt

producer.close(timeout=5)
