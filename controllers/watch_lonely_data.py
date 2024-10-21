from config.redis_server import redis_client
from config.mongo_database import collection
from controllers.store_data_SQL import store_data_in_mysql
import time

def watch_lonely_data():
    """
    Continuously checks for and processes lonely Redis hashes.

    This function runs in an infinite loop to periodically check Redis 
    for hashes with 15+ entries.

    For any found, it extracts the hash data, 
    sorts it, deletes the hash, 
    stores the sorted data in MongoDB and MySQL,
    and prints a message.

    Any errors are printed but the loop continues.

    The periodic check happens every 3 seconds.

    Args:
    None

    Returns:
    None

    Raises:
    Exception: Any uncaught exceptions are printed but
                the loop continues.

    Example:

    ```python  
    watch_lonely_data()
    ```
    """
    while True:
        try:
            keys = redis_client.keys('*')
            for key in keys:
                if redis_client.hlen(key) >= 15:
                    hash_data = redis_client.hgetall(key)
                    merged_data = {key.decode('utf-8'): value.decode('utf-8') for key, value in hash_data.items()}
                    pack_data = dict(sorted(merged_data.items()))
                    redis_client.delete(key)
                    
                    collection.insert_one(pack_data)
                    store_data_in_mysql([pack_data])
                    print("💥 Insert one element in MongoDb and MySQL 💥")

        except Exception as e:
            print(f"Error al procesar datos en Redis: {e}")
        time.sleep(3)