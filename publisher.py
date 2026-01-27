import time
import json
from google.cloud import pubsub_v1
import os


project_id = "networkedapps-fwy-wxs-2026"
topic_id = "scenario1-topic"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account.json"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

for i in range(1, 101):
    data = {
        "source": "publisher-1",
        "timestamp": time.time(),
        "count": i
    }
    data_str = json.dumps(data).encode("utf-8")
    
    future = publisher.publish(topic_path, data_str)
    print(f"Published message {i}, ID: {future.result()}")

    time.sleep(1)

print("Finished publishing.")
