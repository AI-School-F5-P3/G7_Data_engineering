from config.mongo_database import collection

async def store_data_in_mongo(data):
    """
    Stores batched user data in MongoDB.

    This async function takes in a batch of processed user data
    and stores it in the specified MongoDB collection.

    A success message is printed after storing the data.

    Args:
    data (list): The batch of processed user data

    Returns:
    None

    Example:

    ```python
    processed_data = [
        {'name': 'John', 'age': 30}, 
        {'name': 'Jane', 'age': 25}
    ]

    await store_data_in_mongo(processed_data)
    ```
    """
    collection.insert_many(data)
    print('Data stored successfully in Mongo and SQL 🚀')