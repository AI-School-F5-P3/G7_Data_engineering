import os

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'probando')

# MongoDB configuration
MONGODB_CONNECTION_STRING = os.getenv('MONGODB_CONNECTION_STRING', 'mongodb://localhost:27017/')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'hrdata')
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION', 'hr_collection')

# URI configurations
MONGO_URI = os.getenv('MONGO_URI', MONGODB_CONNECTION_STRING)
POSTGRES_URI = os.getenv('POSTGRES_URI', 'postgresql://postgres:luis123@sqldb:5432/hrdata')
REDIS_URI = os.getenv('REDIS_URI', 'redis://redis:6379')