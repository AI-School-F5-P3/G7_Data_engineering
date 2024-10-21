from extraction.kafkaconsumer import KafkaConsumer
from transformation.datatransformer import DataTransformer
from loading.mongodbloader import MongoDBLoader
from loading.sql_loader import SQLLoader
from prometheus_client import start_http_server, Counter
import logging

# Configuración de métricas
messages_processed = Counter('messages_processed_total', 'Total number of messages processed')
errors_counter = Counter('errors_total', 'Total number of errors')

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Inicializar componentes
    kafka_consumer = KafkaConsumer()
    data_transformer = DataTransformer()
    mongodb_loader = MongoDBLoader()
    sql_loader = SQLLoader()

    # Iniciar servidor de Prometheus
    start_http_server(8000)
    logger.info("Prometheus metrics server started")

    # Procesar mensajes
    for message in kafka_consumer.consume():
        try:
            logger.info(f"Received message: {message}")
            # Transformar datos
            processed_data = data_transformer.transform(message)
            
            if processed_data:
                logger.info(f"Processed data: {processed_data}")
                
                # Cargar datos en MongoDB
                mongodb_loader.load(processed_data)

                # Cargar datos en las tablas correspondientes
                if 'passport' in processed_data:
                    if 'name' in processed_data:
                        sql_loader.load_personal_data(processed_data)
                    
                    if 'iban' in processed_data:
                        sql_loader.load_bank_data(processed_data)
                    
                    if 'fullname' in processed_data and 'company' in processed_data:
                        sql_loader.load_professional_data(processed_data)
                    
                    if 'city' in processed_data:
                        sql_loader.load_location_data(processed_data)

                    if 'ipv4' in processed_data:
                        sql_loader.load_net_data(processed_data)

                # Incrementar métrica de mensajes procesados
                messages_processed.inc()
            else:
                logger.info("Incomplete data, waiting for more fragments.")
        except Exception as e:
            errors_counter.inc()
            logger.error(f"Error processing message: {e}")

if __name__ == "__main__":
    main()
