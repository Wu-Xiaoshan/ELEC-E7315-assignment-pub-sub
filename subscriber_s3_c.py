import os
import time
from google.cloud import pubsub_v1

project_id = "networkedapps-fwy-wxs-2026"
subscription_id = "sub-s3-c-test"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    print(f"\n--- Received Message ---")
    print(f"Data: {message.data.decode('utf-8')}")
    
    if message.delivery_attempt:
        print(f"Delivery Attempt: {message.delivery_attempt}")
    else:
        print("Delivery Attempt: 1 (First try)")

    print("CRASHING NOW! (Simulating failure...)")
    os._exit(1) 

print(f"Listening on {subscription_path}...")
with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except Exception as e:
        print(f"Listening failed: {e}")
