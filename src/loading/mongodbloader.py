from pymongo import MongoClient
from config import MONGO_URI

class MongoDBLoader:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client['hrdata']
        self.collection = self.db['hr_collection']

    def load(self, data):
        self.collection.insert_one(data)