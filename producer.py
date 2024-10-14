from kafka import KafkaConsumer
import json

# Set up the Kafka consumer
consumer = KafkaConsumer(
    "probando",  # Replace with your Kafka topic name
    bootstrap_servers=['localhost:29092'],  # Use localhost and port 29092
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Ensure proper deserialization
)

# Consume messages
for msg in consumer:
    print(msg.value)
    print("recibido")