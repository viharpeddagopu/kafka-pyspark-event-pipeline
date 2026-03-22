"""Simulate booking events and publish JSON messages to Kafka (demo producer)."""

import json
import random
import time
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "booking-events"
ROUTES = ("Vizag-Hyderabad", "Hyderabad-Bangalore")
SLEEP_SECONDS = 1
TICKETS_MIN, TICKETS_MAX = 1, 5
PRICE_MIN, PRICE_MAX = 500, 2000


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def random_booking_event() -> dict:
    return {
        "route": random.choice(ROUTES),
        "tickets": random.randint(TICKETS_MIN, TICKETS_MAX),
        "price": random.randint(PRICE_MIN, PRICE_MAX),
        "timestamp": time.time(),
    }


def main() -> None:
    producer = create_producer()
    try:
        while True:
            event = random_booking_event()
            future = producer.send(TOPIC, event)
            future.get(timeout=5)
            print(event)
            time.sleep(SLEEP_SECONDS)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()