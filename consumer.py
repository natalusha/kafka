import json
from time import sleep

from kafka import KafkaConsumer

if __name__ == "__main__":
    parsed_topic_name = "parsed_clothes"

    consumer = KafkaConsumer(
        parsed_topic_name,
        auto_offset_reset="earliest",
        bootstrap_servers=["localhost:9092"],
        api_version=(0, 10),
        consumer_timeout_ms=1000,
    )
    l = []

    for msg in consumer:
        record = json.loads(msg.value)
        l.append(record)
    with open("new_data.json", "w") as f:
        json.dump(l, f, indent=4)

    if consumer is not None:
        consumer.close()
