from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import logging
import json
import os
from src.config import MONGODB_CONNECTION_STRING, MONGODB_DATABASE, MONGODB_COLLECTION

class MongoDBLoader:
    def __init__(self):
        # MongoDB configuration
        self.mongo_uri = MONGODB_CONNECTION_STRING
        self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)  # Timeout for connection
        self.db = self.client[MONGODB_DATABASE]
        self.collection = self.db[MONGODB_COLLECTION]
        self.logger = logging.getLogger(__name__)

    def test_connection(self):
        try:
            self.client.server_info()  # Check if MongoDB is accessible
            self.logger.info("Successfully connected to MongoDB")
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")

    def load(self, data):
        try:
            result = self.collection.insert_one(data)
            self.logger.info(f"Document inserted with ID: {result.inserted_id}")
        except (ConnectionFailure, OperationFailure) as e:
            self.logger.error(f"MongoDB error: {str(e)}")
            # Retry or additional error handling can be added here

# Sample usage of MongoDBLoader
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loader = MongoDBLoader()
    
    # Test the MongoDB connection first
    loader.test_connection()
    
    # Sample data for loading into MongoDB
    sample_data = {
        'name': 'Ms. Hannah',
        'last_name': 'Smith',
        'passport': '004078463',
        'email': 'giulia62@tele2.it',
    }

    # Load data into MongoDB
    loader.load(sample_data)

    # Print the MongoDB URI to verify
    print(f"MongoDB URI: {MONGODB_CONNECTION_STRING}")
