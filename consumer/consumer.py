"""Module listening to Kafka topics to get data."""
import json

from confluent_kafka import Consumer


# The group.id tracks different consumers of the same instance. These are
# brokers/nodes that listen to the same topic, usually for efficiency. This
# is usually done in a production environment to tell the producer that these
# consumers are all working together.
# The auto.offset.reset tells it where to start if by any means, it looses
# track of the last message it read. The earliest in this case means the first
# message received.
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",  # logical grouping
    "auto.offset.reset": "earliest"  # earliest
}

consumer = Consumer(consumer_config)

# Subscribe to the orders topic.
# One consumer can subscribe to more than one topic.
consumer.subscribe(["orders"])

print("🟢 Consumer is running and subscribed to orders topic ...")

# Always check for new message
# Polling allows consumers to control the speed, duration and frequency
# at which the data is processed, rather than Kafka pushing the message
# to the consumers.
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            print("❌ Error: %s", msg.error())
            continue

        value = json.loads(msg.value().decode("utf-8"))
        print(f'📦 Received order: {value["quantity"]} x {value["item"]} from {value["user"]}')
except KeyboardInterrupt:
    print("🛑 Stopping consumer connection.")

finally:
    consumer.close()
    print("⛔ Consumer connection stopped.")
