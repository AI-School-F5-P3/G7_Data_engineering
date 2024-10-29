import logging
import json
from confluent_kafka import Consumer, KafkaException, KafkaError
from prometheus_client import start_http_server, Counter, Summary
from pymongo import MongoClient
from bson import ObjectId
from src.transformation.datatransformer import DataTransformer
from src.loading.mongodbloader import MongoDBLoader
from src.loading.sql_loader import SQLLoader
import urllib.parse as urlparse
import redis
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, POSTGRES_URI, REDIS_URI
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración de métricas de Prometheus
kafka_messages_total = Counter('kafka_messages_total', 'Total de mensajes procesados desde Kafka')
kafka_errors_total = Counter('kafka_errors_total', 'Total de errores en Kafka')
redis_queries_total = Counter('redis_queries_total', 'Total de consultas a Redis')
processing_time = Summary('processing_time', 'Tiempo de procesamiento de cada mensaje')

def convert_objectid_to_str(data):
    """Recorre el diccionario y convierte ObjectId a cadena."""
    for key, value in data.items():
        if isinstance(value, ObjectId):
            data[key] = str(value)
    return data

class KafkaConsumer:
    def __init__(self):
        # Iniciar el servidor de Prometheus en el puerto 8000
        start_http_server(8000)

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
                        kafka_errors_total.inc()  # Registrar error en Kafka
                        raise KafkaException(msg.error())
                else:
                    kafka_messages_total.inc()  # Incrementar contador de mensajes procesados

                    value = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"Received message: {value}")

                    start_time = time.time()  # Iniciar tiempo de procesamiento

                    transformed_data = self.data_transformer.transform(value)
                    
                    if transformed_data:
                        passport_value = transformed_data.get('passport')
                        if passport_value:
                            logger.info(f"Data complete for passport: {passport_value}")
                            transformed_data = convert_objectid_to_str(transformed_data)

                            # Guardar en Redis
                            redis_queries_total.inc()  # Incrementar contador de consultas a Redis
                            self.redis_client.set(passport_value, json.dumps(transformed_data))
                            logger.info(f"Stored data in Redis for passport: {passport_value}")

                            # Cargar en SQL y MongoDB
                            try:
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
                            except KeyError as e:
                                logger.error(f"Error inserting data: missing field {e}")

                            # Cargar en MongoDB
                            self.mongo_loader.load(transformed_data)
                        else:
                            logger.warning("Message does not contain a 'passport' field; skipping SQL and Redis storage.")
                    else:
                        logger.info("Data incomplete, waiting for more fragments.")

                    # Registrar tiempo de procesamiento
                    processing_time.observe(time.time() - start_time)

        except KafkaException as e:
            logger.error(f"Kafka consumption failed: {e}")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = KafkaConsumer()
    consumer.consume()
