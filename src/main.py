import logging
from extraction.kafkaconsumer import KafkaConsumer
from transformation.datatransformer import DataTransformer
from loading.mongodbloader import MongoDBLoader
from loading.sql_loader import SQLLoader
from utils.helper_functions import setup_prometheus

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Inicializar componentes
    kafka_consumer = KafkaConsumer()
    data_transformer = DataTransformer()
    mongodb_loader = MongoDBLoader()
    sql_loader = SQLLoader()

    # Configurar Prometheus
    setup_prometheus()

    # Procesar mensajes
    for message in kafka_consumer.consume():
        try:
            # Transformar datos
            processed_data = data_transformer.transform(message)

            # Cargar en MongoDB
            mongodb_loader.load(processed_data)

            # Cargar en SQL DB
            sql_loader.load(processed_data)

            logger.info(f"Processed message: {processed_data['personal_data'].get('passport')}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

if __name__ == "__main__":
    consumer = KafkaConsumer()
    for message in consumer.consume():
        print(f"Received message: {message}")
