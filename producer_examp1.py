import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer

HOST = "kafka-bruno1-brunofaria.d.aivencloud.com"
SSL_PORT = "13099"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=f"{HOST}:{SSL_PORT}",
    security_protocol="SSL",
    ssl_cafile="/Users/bruno/aiven/certs/ca.pem",
    ssl_certfile="/Users/bruno/aiven/certs/service.cert",
    ssl_keyfile="/Users/bruno/aiven/certs/service.key",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# Function to generate a mock IoT sensor event
def generate_iot_sensor_event():
    event = {
        'sensor_id': f'sensor_{random.randint(1, 1000)}',
        'temperature': round(random.uniform(0.0, 80.0), 2),
        'humidity': round(random.uniform(30.0, 70.0), 2),
        'time_stamp': datetime.utcnow().isoformat()
    }
    return event


# Produce messages to Kafka topic
def produce_message():
    key = json.dumps({'id': str(uuid.uuid4())})
    payload = generate_iot_sensor_event()
    producer.send('iot-sensor-data', key=key.encode('utf-8'), value=payload)
    producer.flush()
    print(f"Payload produced: {payload}")


if __name__ == '__main__':
    for _ in range(5):
        produce_message()
