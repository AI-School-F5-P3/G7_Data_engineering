import logging
import json
from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient
from bson import ObjectId  # Importar para manejar ObjectId
from src.transformation.datatransformer import DataTransformer
from src.loading.mongodbloader import MongoDBLoader
from src.loading.sql_loader import SQLLoader
import urllib.parse as urlparse
import redis
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, POSTGRES_URI, REDIS_URI  # <-- Importar configuración

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Verificación de las variables importadas
logger.info(f"KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
logger.info(f"KAFKA_TOPIC: {KAFKA_TOPIC}")
logger.info(f"POSTGRES_URI: {POSTGRES_URI}")
logger.info(f"REDIS_URI: {REDIS_URI}")

def convert_objectid_to_str(data):
    """Recorre el diccionario y convierte ObjectId a cadena."""
    for key, value in data.items():
        if isinstance(value, ObjectId):
            data[key] = str(value)
    return data

class KafkaConsumer:
    def __init__(self):
        # Usar la variable KAFKA_BOOTSTRAP_SERVERS y KAFKA_TOPIC importadas
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

        # Configuración de Redis
        self.redis_client = redis.StrictRedis.from_url(REDIS_URI)
        logger.info(f"Connected to Redis: {REDIS_URI}")

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
                "database": result.path[1:],  
                "user": result.username,
                "password": result.password
            }
            logger.info(f"Parsed connection info: {parsed_info}")
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
                        logger.error(f"Kafka error: {msg.error()}")
                        raise KafkaException(msg.error())
                else:
                    value = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"Received message: {value}")

                    # Transformar y consolidar datos
                    transformed_data = self.data_transformer.transform(value)

                    if transformed_data:
                        logger.info(f"Data complete for passport: {transformed_data['passport']}")

                        # Convertir ObjectId a cadena antes de guardar en Redis
                        transformed_data = convert_objectid_to_str(transformed_data)

                        # Guardar datos en Redis
                        self.redis_client.set(transformed_data['passport'], json.dumps(transformed_data))
                        logger.info(f"Stored data in Redis for passport: {transformed_data['passport']}")
                    
                        # Llamada a los métodos SQL correspondientes
                        if 'name' in transformed_data:
                            self.sql_loader.load_personal_data(transformed_data)
                        if 'city' in transformed_data:
                            self.sql_loader.load_location_data(transformed_data)
                        if 'company' in transformed_data:
                            self.sql_loader.load_professional_data(transformed_data)
                        if 'iban' in transformed_data:
                            self.sql_loader.load_bank_data(transformed_data)
                        if 'ipv4' in transformed_data:
                            self.sql_loader.load_net_data(transformed_data)
                    
                        # Cargar en MongoDB
                        self.mongo_loader.load(transformed_data)
                    else:
                        logger.info("Data incomplete, waiting for more fragments.")
        except KafkaException as e:
            logger.error(f"Kafka consumption failed: {e}")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = KafkaConsumer()
    consumer.consume()
