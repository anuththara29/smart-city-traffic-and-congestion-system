# sensor_producer.py
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ["127.0.0.1:9092"]
TOPIC = "traffic-sensors"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    api_version=(0,10,2),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

SENSORS = ["JUNCTION-1", "JUNCTION-2", "JUNCTION-3", "JUNCTION-4"]

def generate_normal_reading(sensor_id):
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "vehicle_count": random.randint(0, 10),
        "avg_speed": round(random.uniform(30.0, 60.0), 2)
    }

def generate_critical_reading(sensor_id):
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "vehicle_count": random.randint(10, 30),
        "avg_speed": round(random.uniform(3.0, 9.5), 2)
    }

if __name__ == "__main__":
    print("Starting sensor producer; sending to topic:", TOPIC)
    try:
        while True:
            for s in SENSORS:
                if random.random() < 0.03:
                    msg = generate_critical_reading(s)
                else:
                    msg = generate_normal_reading(s)
                producer.send(TOPIC, msg)
                print(f"Produced -> {msg}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()
