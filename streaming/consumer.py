from kafka import KafkaConsumer
import json
import time
from datetime import datetime, timezone

consumer = None

# --- wait for Kafka ---
while consumer is None:
    try:
        print("Trying to connect to Kafka...")
        consumer = KafkaConsumer(
            "openmeteo_current",
            bootstrap_servers="kafka:9092",
            group_id="weather-prediction",
            auto_offset_reset="latest",
            value_deserializer=lambda m: m.decode("utf-8", errors="ignore"),
        )
        print("Connected to Kafka!")
    except Exception as e:
        print("Kafka not ready yet, retrying in 5s...", e)
        time.sleep(5)

print("Streaming consumer started")

# --- consume messages ---
for msg in consumer:
    raw = msg.value.strip()

    if not raw:
        continue

    try:
        weather = json.loads(raw)
    except Exception:
        # not a JSON message → skip
        continue

    temperature = weather.get("temperature") or weather.get("temperature_2m", 0)
    wind_speed = weather.get("windspeed") or weather.get("wind_speed_10m", 0)

    prediction = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "input_weather": weather,
        "predicted_solar": max(0, temperature),
        "predicted_wind": max(0, wind_speed),
        "model": "baseline-stream-v1"
    }

    print(json.dumps(prediction, indent=2))
