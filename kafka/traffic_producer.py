from kafka import KafkaProducer
from faker import Faker
import json
import time
import random

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_traffic_data():
    return {
        'timestamp': fake.iso8601(),
        'location': {
            'lat': round(random.uniform(53.5, 53.6), 6),
            'lon': round(random.uniform(9.9, 10.1), 6)
        },
        'speed': random.randint(0, 80),
        'vehicle_count': random.randint(1, 10)
    }

while True:
    data = generate_traffic_data()
    producer.send('hamburg_traffic', value=data)
    print("Sent:", data)
    time.sleep(1)
