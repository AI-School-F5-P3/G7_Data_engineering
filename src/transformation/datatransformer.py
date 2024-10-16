import redis
import json
import logging

class DataTransformer:
    def __init__(self, redis_host="localhost", redis_port=6379, redis_db=0):
        # Inicializamos la conexión con Redis
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
        self.logger = logging.getLogger(__name__)
    
    def transform(self, message):
        passport = message.get('passport')

        if not passport:
            self.logger.warning("Message without passport cannot be processed.")
            return None

        # Cargar los datos actuales desde Redis
        redis_key = f"user:{passport}"
        cached_data = self.redis_client.get(redis_key)
        
        if cached_data:
            # Si hay datos previos en Redis, los combinamos
            user_data = json.loads(cached_data)
        else:
            user_data = {}

        # Unir los datos fragmentados
        for key, value in message.items():
            if key and value:
                user_data[key] = value

        # Verificamos si los datos están completos
        if self.is_data_complete(user_data):
            self.redis_client.delete(redis_key)  # Eliminamos el cache si los datos están completos
            return user_data
        else:
            # Si los datos no están completos, los volvemos a guardar en Redis
            self.redis_client.set(redis_key, json.dumps(user_data))
            self.logger.info(f"Partial data for passport {passport} stored in Redis.")
            return None

    def is_data_complete(self, user_data):
        required_fields = ['passport', 'name', 'last_name', 'email', 'salary', 'IBAN']  # Ajusta los campos requeridos según tu necesidad
        return all(field in user_data for field in required_fields)
