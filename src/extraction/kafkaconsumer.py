import logging
from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from pymongo import MongoClient
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, MONGODB_CONNECTION_STRING, MONGODB_DATABASE, MONGODB_COLLECTION

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConsumer:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'hr_group',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")
        
        self.mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
        self.db = self.mongo_client[MONGODB_DATABASE]
        self.collection = self.db[MONGODB_COLLECTION]
        logger.info(f"Connected to MongoDB: {MONGODB_DATABASE}.{MONGODB_COLLECTION}")

    def consume(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("Reached end of partition")
                        continue
                    else:
                        raise KafkaException(msg.error())
                else:
                    value = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"Received message: {value}")
                    self.save_to_mongodb(value)
                    yield value
        
        finally:
            self.consumer.close()
            self.mongo_client.close()

    def save_to_mongodb(self, message):
        try:
            result = self.collection.insert_one(message)
            logger.info(f"Message saved to MongoDB with ID: {result.inserted_id}")
        except Exception as e:
            logger.error(f"Error saving to MongoDB: {e}")

if __name__ == "__main__":
    consumer = KafkaConsumer()
    for message in consumer.consume():
        logger.info(f"Processing message: {message}")
        # El mensaje ya se guarda en MongoDB en el método consume