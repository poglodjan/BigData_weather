import json
from kafka import KafkaProducer, KafkaConsumer


def test_weather_stream_to_kafka():
    """
    Test objective:
    Verify that a weather data message can be published to a Kafka topic
    and consumed by a Kafka consumer.

    This test focuses on validating the intended producer-consumer logic
    rather than full production execution.
    """

    # Kafka producer configuration
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Sample weather data message
    weather_event = {
        "time": "2025-01-01T12:00:00Z",
        "temperature_2m": 4.8,
        "wind_speed_10m": 6.3,
        "cloud_cover": 75,
    }

    # Publish message to Kafka topic
    producer.send("weather_current", weather_event)
    producer.flush()

    # Kafka consumer configuration
    consumer = KafkaConsumer(
        "weather_current",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=5000,
    )

    # Consume messages
    messages = [msg.value for msg in consumer]

    # Basic validation
    assert len(messages) > 0
    assert "temperature_2m" in messages[-1]
    assert messages[-1]["wind_speed_10m"] == 6.3