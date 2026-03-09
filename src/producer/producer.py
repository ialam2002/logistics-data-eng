import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os
from dotenv import load_dotenv

# Load environment variables from .env when running locally
load_dotenv()

broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
# Connect to the kafka broker (retry while broker isn't ready)
producer = None
max_attempts = 30
attempt = 0
while attempt < max_attempts and producer is None:
    try:
        attempt += 1
        print(f"Connecting to Kafka broker ({broker}) - attempt {attempt}/{max_attempts}...")
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except NoBrokersAvailable:
        print("No brokers available yet - sleeping 2s and retrying...")
        time.sleep(2)

if producer is None:
    raise RuntimeError(f"Could not connect to Kafka broker at {broker} after {max_attempts} attempts")

def generate_truck_data():
    truck_ids = ['TRUCK-001', 'TRUCK-002', 'TRUCK-003']
    return {
        "truck_id": random.choice(truck_ids),
        "lat": round(random.uniform(40.7128, 40.8000), 4), # NYC area
        "lon": round(random.uniform(-74.0060, -73.9000), 4),
        "status": random.choice(["moving", "idling", "loading"]),
        "timestamp": time.time()
    }

print("Streaming logistics data to Kafka... Press Ctrl+C to stop.")
try:
    while True:
        data = generate_truck_data()
        producer.send('logistics-stream', value=data)
        print(f"Sent: {data}")
        time.sleep(2) # Send data every 2 seconds
except KeyboardInterrupt:
    print("Streaming stopped.")