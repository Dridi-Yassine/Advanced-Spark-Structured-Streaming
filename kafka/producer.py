<<<<<<< HEAD
import json
import time
from kafka import KafkaProducer
import os

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPIC = 'iot-events'
DATA_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'events_dirty.json')

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: str(v).encode('utf-8')
        )
        return producer
    except Exception as e:
        print(f"Error creating producer: {e}")
        return None

def produce_events():
    producer = create_producer()
    if not producer:
        return

    print(f"Reading from {DATA_FILE}...")
    try:
        with open(DATA_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    # Publish the raw line to Kafka (to simulate real streaming ingestion)
                    producer.send(KAFKA_TOPIC, value=line)
                    print(f"Sent: {line[:50]}...")
                    time.sleep(0.1) # Simulate real-time stream
        producer.flush()
        print("Finished sending all events.")
    except FileNotFoundError:
        print(f"Data file not found: {DATA_FILE}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    produce_events()
=======
# Write your kafka producer code here
>>>>>>> 98a843ef9356e74eaa4bf0fea8e8c9beafb55213
