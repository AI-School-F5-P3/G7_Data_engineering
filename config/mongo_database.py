from config.connection import MONGO_URI, MONGO_DATABASE, MONGO_COLLECTION
from pymongo import MongoClient

# Create an instance of MongoDb
client = MongoClient(MONGO_URI)
db = client[MONGO_DATABASE]
collection = db[MONGO_COLLECTION]