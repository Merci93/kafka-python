import json
import random
import time
import uuid

from confluent_kafka import Producer
from faker import Faker


FAKER = Faker()

# Producer confiuration.
producer_config = {
    "bootstrap.servers": "localhost:9092",
}

producer = Producer(producer_config)


def delivery_message(err, msg) -> None:
    """A callback function to notify of message delivery or failure."""
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f'✅ Delivered {msg.value().decode("utf-8")}')


products = [
    "Wireless Bluetooth Headphones",
    "Mechanical Gaming Keyboard",
    "27-Inch 4K Monitor",
    "USB-C Docking Station",
    "Portable SSD 1TB",
    "Smart Fitness Watch",
    "Ergonomic Office Chair",
    "Adjustable Standing Desk",
    "LED Desk Lamp",
    "Noise Cancelling Earbuds",
    "Stainless Steel Water Bottle",
    "Electric Coffee Grinder",
    "Smart Home Security Camera",
    "Air Purifier",
    "Robot Vacuum Cleaner",
    "Wireless Charging Pad",
    "Portable Power Bank",
    "Full HD Webcam",
    "External DVD Drive",
    "Laptop Cooling Pad"
]


# Create an order event
def generate_fake_data() -> str:
    """A function to generate fake data"""
    order_data = {
        "order_id": str(uuid.uuid4()),
        "user": FAKER.first_name(),
        "item": random.choice(products),
        "quantity": random.randrange(0, 20),
    }

    # Convert dictionary data to string and encode in bytes
    # which Kafka understands
    return json.dumps(order_data).encode("utf-8")


# We are sending this to the orders topic.
# If the topic is not yet created, kafka will automatically create the topic
# and append the data to the topic.
try:
    while True:
        producer.produce(
            topic="orders",
            value=generate_fake_data(),
            callback=delivery_message,
        )

        # This sends data in batches, and if by chance this producer crashes,
        # it ensures the last batch in process is sent before the producer is
        # exited. This is buffering and helps in performance in production and
        # a best practive. In a production environment, the flush would not be
        # at this stage, only at the exit point. This here is for learning
        # purpose so the message can be displayed.
        producer.flush()
        time.sleep(8)

except KeyboardInterrupt:
    print("🛑 Terminating producer ...")

finally:
    producer.close()
    print("⛔ Producer stopped.")
