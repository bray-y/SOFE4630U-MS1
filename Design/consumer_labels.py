from google.cloud import pubsub_v1
import json
import glob
import os

# Load service account credentials
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set project and subscription
project_id = "mobile-dev-proj-479023" 
topic_name = "labelsTopic"
subscription_name = "labelsSub"

subscriber = pubsub_v1.SubscriberClient()
topic_path = subscriber.topic_path(project_id, topic_name)
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# Create subscription if it does not exist
try:
    subscriber.create_subscription(
        name=subscription_path,
        topic=topic_path
    )
except Exception:
    pass  # subscription already exists

print(f"Listening for messages on {subscription_path}...\n")

def callback(message):
    data = message.data.decode("utf-8")
    record = json.loads(data)

    print("Received record:")
    for key, value in record.items():
        print(f"  {key}: {value}")
    print("-" * 30)

    message.ack()

subscriber.subscribe(subscription_path, callback=callback)

# Keep the consumer running
import time
while True:
    time.sleep(5)
