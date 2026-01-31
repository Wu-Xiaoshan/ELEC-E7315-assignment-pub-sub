import time
from google.cloud import pubsub_v1

project_id = "networkedapps-fwy-wxs-2026"
topic_id = "scenario1-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

print(f"Publishing message to {topic_path}...")

data = "POISON PILL: This message will crash the subscriber 5 times!".encode("utf-8")
future = publisher.publish(topic_path, data)
print(f"Published message ID: {future.result()}")

print("Done.")
