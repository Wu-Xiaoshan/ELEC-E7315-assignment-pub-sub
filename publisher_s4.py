import time
import json
import sys
import os
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import LimitExceededBehavior
from concurrent import futures

project_id = "networkedapps-fwy-wxs-2026"
topic_id = "scenario1-topic"

if len(sys.argv) < 2:
    print("Usage: python publisher_s4.py <case>")
    print("case: A (Limit 10, Block), B (Limit 10, Error), C (Limit 1000)")
    sys.exit(1)

case = sys.argv[1].upper()

if case == 'A':
    # Case A
    message_limit = 10
    limit_behavior = LimitExceededBehavior.BLOCK
    print("--- Running Case A: Limit=10, Behavior=BLOCK ---")
elif case == 'B':
    # Case B
    message_limit = 10
    limit_behavior = LimitExceededBehavior.ERROR
    print("--- Running Case B: Limit=10, Behavior=ERROR ---")
else:
    # Case C
    message_limit = 1000
    limit_behavior = LimitExceededBehavior.BLOCK
    print("--- Running Case C: Limit=1000 (Default) ---")

flow_control_settings = pubsub_v1.types.PublishFlowControl(
    message_limit=message_limit,
    limit_exceeded_behavior=limit_behavior,
)

publisher_options = pubsub_v1.types.PublisherOptions(flow_control=flow_control_settings)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
topic_path = publisher.topic_path(project_id, topic_id)

publish_futures = []

print("Starting to publish 100 messages asynchronously...")
start_time = time.time()

for i in range(1, 101):
    data = {"count": i, "timestamp": time.time()}
    data_str = json.dumps(data).encode("utf-8")
    
    try:
        future = publisher.publish(topic_path, data_str)
        publish_futures.append(future)
        
        if i % 10 == 0:
            print(f"Scheduled message {i}")
            
    except Exception as e:
        print(f"!! Error publishing message {i}: {e}")

print("Waiting for futures to complete...")
try:
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
except Exception as e:
    print(f"Error during wait: {e}")

end_time = time.time()
print(f"Finished. Total time: {end_time - start_time:.4f}s")
