import json
from config.redis_server import redis_client
from controllers.send_data import sort_and_send_data
from controllers.classify_data import classify_data
from controllers.create_hash import create_hash_for_passport, create_hash_for_address, create_hash_for_fullname

async def start_streaming(probando_stream):
    """
    Starts streaming and processing user data.

    This async function takes in a stream of user data. 
    It counts the records, classifies each one, creates Redis hashes,
    batches data, and sends batches to MongoDB and MySQL.

    The data is classified based on passport, address, or fullname.
    Corresponding Redis hashes are created for each classified record.

    Batched data is sent when the batch size reaches 5 records.

    Any JSON or other errors are printed.

    Args:
    probando_stream: The stream of input user data

    Returns:
    None

    Raises:
    json.JSONDecodeError: If any JSON parsing errors occur

    Example:

    ```python
    stream = get_user_data_stream()

    await start_streaming(stream)
    ```
"""

    counter = 0
    pack_data = []

    async for data in probando_stream:
        counter += 1
        print(f"- {counter} -")
        try:
            key_type, key, processed_data = await classify_data(data)
            if key_type and key and processed_data:
                redis_key = f'{key_type}: {key}'
                if key_type == 'passport':
                    await create_hash_for_passport(redis_client, redis_key, processed_data)
                elif key_type == 'address':
                    await create_hash_for_address(redis_client, redis_key, processed_data)
                elif key_type == 'fullname':
                    await create_hash_for_fullname(redis_client, redis_key, processed_data)
                await sort_and_send_data(processed_data, pack_data)

        except json.JSONDecodeError as e:
            print(f"Error al decodificar JSON: {e}, Data: {data}")