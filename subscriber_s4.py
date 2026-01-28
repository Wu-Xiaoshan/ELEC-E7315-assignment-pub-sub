import time
import json
import sys
import os
from google.cloud import pubsub_v1

project_id = "networkedapps-fwy-wxs-2026"
subscription_id = "sub-s1-finland"

max_messages = 1000
if len(sys.argv) > 1:
    max_messages = int(sys.argv[1])

print(f"--- Subscriber Flow Control: max_messages={max_messages} ---")

flow_control = pubsub_v1.types.FlowControl(max_messages=max_messages)

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

received_count = 0
start_receive_time = 0

def callback(message):
    global received_count, start_receive_time
    
    if received_count == 0:
        start_receive_time = time.time()
        
    received_count += 1
    message.ack()
    
    if received_count % 10 == 0:
        print(f"Received {received_count}/100")

    if received_count >= 100:
        end_time = time.time()
        duration = end_time - start_receive_time
        print(f"\n[DONE] Received 100 messages.")
        print(f"Total time taken: {duration:.4f}s")

print(f"Listening on {subscription_id}...")

streaming_pull_future = subscriber.subscribe(
    subscription_path, 
    callback=callback,
    flow_control=flow_control
)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\nStopped by user.")
