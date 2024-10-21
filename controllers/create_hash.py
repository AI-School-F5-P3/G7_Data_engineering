from config.redis_server import redis_client

async def create_hash_for_passport(redis_client, key, data):
    """
    Creates a hash in Redis for passport data.

    This function takes in a Redis client, key, and data dict. 
    It adds the data to a hash at the given key. If the hash grows 
    past 5 entries, it creates a new hash using the fullname as the key,
    copies the data over, and deletes the original hash.

    Args:
    redis_client: The Redis client instance
    key (str): The key for the original hash
    data (dict): The data to add to the hash

    Returns:
    None

    Example:
    ```python  
    import redis

    r = redis.Redis(...)
    key = 'passport_data'
    data = {'name': 'John', 'age': 30}

    create_hash_for_passport(r, key, data)
    ```
"""

    redis_client.hset(key, mapping=data)
    if redis_client.hlen(key) > 5:
        hash_data = redis_client.hgetall(key)
        new_fullname = hash_data.get(b'fullname').decode('utf-8')
        new_fullname_hash = f'fullname: {new_fullname}'
        redis_client.hset(new_fullname_hash, mapping=hash_data)
        redis_client.delete(key)
        
async def create_hash_for_address(redis_client, key, data):
    redis_client.hset(key, mapping=data)
    if redis_client.hlen(key) > 3:
        hash_data = redis_client.hgetall(key)
        new_fullname = hash_data.get(b'fullname').decode('utf-8')
        new_fullname_hash = f'fullname: {new_fullname}'
        redis_client.hset(new_fullname_hash, mapping=hash_data)
        redis_client.delete(key)

async def create_hash_for_fullname(redis_client, key, data):
    redis_client.hset(key, mapping=data)