import json
import time
import random
from kafka import KafkaProducer

# Connect to the kafka broker started in Docker
producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_truck_data():
    truck_ids = ['TRUCK-001', 'TRUCK-002', 'TRUCK-003']
    return {
        "truck_id": random.choice(truck_ids),
        "lat": round(random.uniform(40.7128, 40.8000), 4), # NYC area
        "lon": round(random.uniform(-74.0060, -73.9000), 4),
        "status": random.choice(["moving", "idling", "loading"]),
        "timestamp": time.time()
    }

print("Streaming logistics data to Redpanda... Press Ctrl+C to stop.")
try:
    while True:
        data = generate_truck_data()
        producer.send('logistics-stream', value=data)
        print(f"Sent: {data}")
        time.sleep(2) # Send data every 2 seconds
except KeyboardInterrupt:
    print("Streaming stopped.")