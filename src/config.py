import os

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_TOPIC = 'probando'
MONGODB_CONNECTION_STRING = 'mongodb://localhost:27017/'
MONGODB_DATABASE = 'hrdata'
MONGODB_COLLECTION = 'hr_collection'  # Cambiado a 'hr_collection' para coincidir con tu estructura

# Otras configuraciones...
MONGO_URI = os.getenv('MONGO_URI', MONGODB_CONNECTION_STRING)
POSTGRES_URI = os.getenv('POSTGRES_URI', 'postgresql://postgres:luis123@sqldb:5432/hrdata')
REDIS_URI = os.getenv('REDIS_URI', 'redis://redis:6379')