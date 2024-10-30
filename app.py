from confluent_kafka import Consumer, KafkaError
import threading
import json
from controllers.start_streaming import start_streaming
from config.mongo_database import collection
from controllers.create_structure_SQL import create_mysql_table
from controllers.watch_lonely_data import watch_lonely_data
from config.connection import KAFKA_HOST, KAFKA_PORT

# Kafka configuration settings
kafka_config = {
    'bootstrap.servers': f'{KAFKA_HOST}:{KAFKA_PORT}',
    'group.id': 'my-kafka-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Initialize Kafka consumer
consumer = Consumer(kafka_config)
topic_name = 'probando'
consumer.subscribe([topic_name])

# Start creating MySQL table structure
create_mysql_table()

def consume_messages():
    """Consume messages from Kafka and process them."""
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Deserialize JSON message
            message_value = json.loads(msg.value().decode('utf-8'))
            # Process the message using the start_streaming function
            start_streaming(message_value)
    finally:
        consumer.close()

if __name__ == '__main__':
    # Start a separate thread for watching lonely data
    redis_thread = threading.Thread(target=watch_lonely_data)
    redis_thread.start()

    # Start consuming messages
    consume_messages()

