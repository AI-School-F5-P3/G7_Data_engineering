import logging
import json
from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, MONGODB_CONNECTION_STRING, MONGODB_DATABASE, MONGODB_COLLECTION, POSTGRES_URI
from src.transformation.datatransformer import DataTransformer
from src.loading.mongodbloader import MongoDBLoader
from src.loading.utils.helper_functions import setup_prometheus
from src.loading.sql_loader import SQLLoader
import urllib.parse as urlparse

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

        self.mongo_loader = MongoDBLoader()
        self.sql_loader = self.initialize_sql_loader()
        self.data_transformer = DataTransformer()

    def initialize_sql_loader(self):
        try:
            conn_info = self.parse_postgres_uri(POSTGRES_URI)
            return SQLLoader(conn_info)
        except Exception as e:
            logger.error(f"Error initializing SQLLoader: {str(e)}")
            raise

    def parse_postgres_uri(self, uri):
        try:
            result = urlparse.urlparse(uri)
            parsed_info = {
                "host": result.hostname,
                "port": result.port,
                "database": result.path[1:],  # Removes the leading '/' from the path
                "user": result.username,
                "password": "****"  # Mask the password in logs
            }
            logger.info(f"Parsed connection info: {parsed_info}")
            
            # Set the actual password (not logged)
            parsed_info["password"] = result.password
            
            return parsed_info
        except Exception as e:
            logger.error(f"Error parsing Postgres URI: {str(e)}")
            raise

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

                    # Transformar y consolidar datos
                    transformed_data = self.data_transformer.transform(value)
                    
                    if transformed_data:
                        logger.info(f"Data complete for passport: {transformed_data['passport']}")
                        
                        # Cargar en MongoDB y SQL
                        self.mongo_loader.load(transformed_data)
                        self.sql_loader.load(transformed_data)
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = KafkaConsumer()
    consumer.consume()