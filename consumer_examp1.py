import json
from kafka import KafkaConsumer

HOST = "kafka-bruno1-brunofaria.d.aivencloud.com"
SSL_PORT = "13099"

consumer = KafkaConsumer(
    auto_offset_reset="earliest",
    bootstrap_servers=f"{HOST}:{SSL_PORT}",
    #client_id = CONSUMER_CLIENT_ID,
    # group_id='iot-sensor-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    # key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    security_protocol="SSL",
    ssl_cafile="/Users/bruno/aiven/certs/ca.pem",
    ssl_certfile="/Users/bruno/aiven/certs/service.cert",
    ssl_keyfile="/Users/bruno/aiven/certs/service.key",
    # consumer_timeout_ms=1000,
)

# consumer.subscribe(['iot-sensor-data'])
# consumer.subscribe(['high_temp_sensors'])
consumer.subscribe(['low_temp_sensors'])
for message in consumer:
    # key = message.key.decode('utf-8')
    value = message.value
    print(f"Consumed event with value: {value}")
    # print(message.value.decode('utf-8'))

consumer.close()
