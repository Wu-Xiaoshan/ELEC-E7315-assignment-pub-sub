import time
import json
import numpy as np
import os
import sys
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

project_id = "networkedapps-fwy-wxs-2026"

if len(sys.argv) < 2:
    print("Usage: python subscriber.py <subscription_name>")
    sys.exit(1)

subscription_id = sys.argv[1]

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account.json"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

latencies = []

def callback(message):
    receive_time = time.time()
    
    try:
        data = json.loads(message.data.decode("utf-8"))
        send_time = data["timestamp"]
        msg_count = data["count"]
        
        latency = receive_time - send_time
        latencies.append(latency)
        
        print(f"Received msg {msg_count}, Latency: {latency:.4f}s")
        
        message.ack()
        
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

print(f"Listening for messages on {subscription_path}...\n")

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
#except TimeoutError:
#    streaming_pull_future.cancel()
#    print("\nTimeout reached, stopping listener.")
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\nStopped by user.")

if latencies:
    print("\n" + "="*30)
    print(f"RESULTS for {subscription_id}")
    print("="*30)
    print(f"Messages received: {len(latencies)}")
    print(f"Average Latency: {np.mean(latencies):.4f} s")
    print(f"Min Latency:     {np.min(latencies):.4f} s")
    print(f"Max Latency:     {np.max(latencies):.4f} s")
    print(f"Std Deviation:   {np.std(latencies):.4f} s")
    print(f"P50 (Median):    {np.percentile(latencies, 50):.4f} s")
    print(f"P95:             {np.percentile(latencies, 95):.4f} s")
    print(f"P99:             {np.percentile(latencies, 99):.4f} s")
else:
    print("\nNo messages received.")
