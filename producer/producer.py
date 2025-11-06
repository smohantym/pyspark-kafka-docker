import json
import os
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "events")
RATE = float(os.getenv("MSGS_PER_SEC", "5"))

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8") if v else None,
)

print(f"Producing to {BOOTSTRAP} topic={TOPIC} at ~{RATE} msgs/sec")

i = 0
try:
    while True:
        i += 1
        msg = {
            "event_id": f"evt-{i}",
            "ts": datetime.now(timezone.utc).isoformat(),
            "value": round(random.uniform(0, 100), 3),
            "source": "demo-producer",
        }
        producer.send(TOPIC, key=msg["event_id"], value=msg)
        time.sleep(1.0 / RATE if RATE > 0 else 0)
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.flush()
    producer.close()
