from kafka import KafkaConsumer
import json

# Decode Streamed Data from bytes back to JSON and then to Dictionary
consumer = KafkaConsumer("vehicle_positions",
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                         bootstrap_servers='localhost:9092')
# Print received value
for message in consumer:
    print(f"Received message: {message.value}")
