# alerts_consumer.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "Critical-Traffic",
    bootstrap_servers=["127.0.0.1:9092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=1000
)

print("Listening for alerts on 'Critical-Traffic' (press Ctrl+C to quit)...")
try:
    while True:
        for msg in consumer:
            print("ALERT:", json.dumps(msg.value, indent=2))
except KeyboardInterrupt:
    print("Stopped.")
finally:
    consumer.close()
