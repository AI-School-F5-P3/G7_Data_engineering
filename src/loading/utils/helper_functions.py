from prometheus_client import start_http_server, Counter, Histogram

MESSAGES_PROCESSED = Counter('messages_processed', 'Number of messages processed')
PROCESSING_TIME = Histogram('processing_time', 'Time spent processing messages')

def setup_prometheus():
    start_http_server(8000)  # Iniciar servidor de métricas de Prometheus