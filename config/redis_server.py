from config.connection import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
import redis

# Create an instance of Redis
redis_client = redis.Redis(
    host=redis,
    port=REDIS_PORT,
    password=REDIS_PASSWORD)