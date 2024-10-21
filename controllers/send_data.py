from config.redis_server import redis_client
from controllers.store_data_mongo import store_data_in_mongo
from controllers.store_data_SQL import store_data_in_mysql

async def sort_and_send_data(data_list, pack_data):
    """
    Sorts and sends batched user data to MongoDB and MySQL.

    This async function checks for a 'fullname' key in the input data.
    If found, it gets the corresponding Redis hash data if it has 15 entries.

    The hash data is converted to a sorted dict and appended to a pack_data list. 
    When pack_data reaches 5 entries, it is sent to MongoDB and MySQL storage.

    Any errors are printed.

    Args:
    data_list (dict): The input user data 
    pack_data (list): The batch data list

    Returns:
    None

    Raises:
    Exception: Any errors sending data are raised.

    Example:
    ```python
    data = {'fullname': 'John Doe'}

    pack_data = []

    await sort_and_send_data(data, pack_data)
    ```
"""

    try:
        if 'fullname' in data_list:
            fullname = data_list['fullname']
            redis_key = f'fullname: {fullname}'
            
            if redis_client.hlen(redis_key) == 15:
                hash_data = redis_client.hgetall(redis_key)
                merged_data = {key.decode('utf-8'): value.decode('utf-8') for key, value in hash_data.items()}
                pack_data.append(dict(sorted(merged_data.items())))
                redis_client.delete(redis_key)

                if len(pack_data) == 5:
                    await store_data_in_mongo(pack_data)
                    store_data_in_mysql(pack_data)
                    pack_data.clear()
    except Exception as e:
        print("No se ha podido enviar a MongoDB", {e})