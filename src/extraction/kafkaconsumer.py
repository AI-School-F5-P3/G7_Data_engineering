import logging
import json
from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, MONGO_URI, POSTGRES_URI, REDIS_URI
from src.transformation.datatransformer import DataTransformer
from src.loading.mongodbloader import MongoDBLoader
from src.loading.sql_loader import SQLLoader
from prometheus_client import Counter
import urllib.parse as urlparse
import redis

# Configuración de métricas
messages_processed = Counter('messages_processed_total', 'Total number of messages processed')
errors_counter = Counter('errors_total', 'Total number of errors')

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConsumer:
    def __init__(self):
        # Configuración de Kafka
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'hr_group',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")

        # Cargar MongoDB, PostgreSQL y Redis
        self.mongo_loader = MongoDBLoader()
        self.sql_loader = self.initialize_sql_loader()
        self.redis_client = self.initialize_redis()

        # Transformador de datos
        self.data_transformer = DataTransformer()

    def initialize_sql_loader(self):
        try:
            conn_info = self.parse_postgres_uri(POSTGRES_URI)
            return SQLLoader(conn_info)
        except Exception as e:
            logger.error(f"Error initializing SQLLoader: {str(e)}")
            raise

    def initialize_redis(self):
        try:
            # Inicializa el cliente Redis
            redis_client = redis.StrictRedis.from_url(REDIS_URI)
            logger.info(f"Connected to Redis: {REDIS_URI}")
            return redis_client
        except Exception as e:
            logger.error(f"Error connecting to Redis: {str(e)}")
            raise

    def parse_postgres_uri(self, uri):
        try:
            result = urlparse.urlparse(uri)
            parsed_info = {
                "host": result.hostname,
                "port": result.port,
                "database": result.path[1:],  # Remueve el '/' inicial
                "user": result.username,
                "password": "****"  # Enmascara la contraseña en logs
            }
            logger.info(f"Parsed connection info: {parsed_info}")
            
            # Se setea la contraseña real (sin log)
            parsed_info["password"] = result.password
            return parsed_info
        except Exception as e:
            logger.error(f"Error parsing Postgres URI: {str(e)}")
            raise

    def cache_message_in_redis(self, key, message):
        try:
            # Almacena el mensaje en Redis
            self.redis_client.set(key, json.dumps(message))
            logger.info(f"Message cached in Redis with key: {key}")
        except Exception as e:
            logger.error(f"Error caching message in Redis: {str(e)}")

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
                    # Deserializa el mensaje y procesa
                    value = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"Received message: {value}")

                    # Cachear el mensaje en Redis antes de procesar
                    self.cache_message_in_redis(f"msg:{value.get('passport', 'unknown')}", value)

                    # Transformar y consolidar datos
                    transformed_data = self.data_transformer.transform(value)

                    if transformed_data:
                        logger.info(f"Data complete for passport: {transformed_data['passport']}")
                        
                        # Cargar en MongoDB y SQL
                        self.mongo_loader.load(transformed_data)
                        if 'name' in transformed_data:
                            self.sql_loader.load_personal_data(transformed_data)
                        if 'iban' in transformed_data:
                            self.sql_loader.load_bank_data(transformed_data)
                        if 'company' in transformed_data:
                            self.sql_loader.load_professional_data(transformed_data)
                        if 'city' in transformed_data:
                            self.sql_loader.load_location_data(transformed_data)
                        if 'ipv4' in transformed_data:
                            self.sql_loader.load_net_data(transformed_data)

                        # Incrementar métricas de Prometheus
                        messages_processed.inc()
                    else:
                        logger.info("Data incomplete, waiting for more fragments.")
        except KafkaException as e:
            errors_counter.inc()
            logger.error(f"Kafka consumption failed: {e}")
        finally:
            self.consumer.close()

