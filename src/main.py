import logging
from extraction.kafkaconsumer import KafkaConsumer
from transformation.datatransformer import DataTransformer
from loading.mongodbloader import MongoDBLoader
from loading.sql_loader import SQLLoader
from prometheus_client import start_http_server, Counter
import sys
import os

# Añadir la raíz del proyecto al PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configuración de métricas
messages_processed = Counter('messages_processed_total', 'Total number of messages processed')
errors_counter = Counter('errors_total', 'Total number of errors')

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar el servidor Prometheus en el puerto 8000
start_http_server(8000)
logger.info("Prometheus metrics server started")

def main():
    # Inicializar componentes
    kafka_consumer = KafkaConsumer()
    data_transformer = DataTransformer()
    mongodb_loader = MongoDBLoader()
    sql_loader = SQLLoader()

    # Procesar mensajes
    for message in kafka_consumer.consume():
        try:
            logger.info(f"Received message: {message}")
            # Transformar datos
            processed_data = data_transformer.transform(message)
            
            if processed_data:  # Asegúrate de que los datos no sean `None`
                logger.info(f"Processed data: {processed_data}")
                # Cargar en MongoDB
                mongodb_loader.load(processed_data)

                # Cargar en SQL DB
                sql_loader.load(processed_data)

                # Incrementar la métrica de mensajes procesados
                messages_processed.inc()
                logger.info(f"Processed message: {processed_data['passport']}")
            else:
                logger.info("Incomplete data, waiting for more fragments.")
        except Exception as e:
            # Incrementar la métrica de errores
            errors_counter.inc()
            logger.error(f"Error processing message: {e}")

if __name__ == "__main__":
    main()
