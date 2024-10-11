from pymongo import MongoClient
import os
from src.config import MONGODB_CONNECTION_STRING, MONGODB_DATABASE, MONGODB_COLLECTION

class MongoDBLoader:
    def __init__(self):
        # Usar la configuración de config.py
        self.mongo_uri = MONGODB_CONNECTION_STRING
        
        # Establecer la conexión con MongoDB
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[MONGODB_DATABASE]
        self.collection = self.db[MONGODB_COLLECTION]

    def test_connection(self):
        try:
            self.client.server_info()
            print("Successfully connected to MongoDB")
        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}")

    def load(self, data):
        try:
            # Intentar insertar el documento
            result = self.collection.insert_one(data)
            print(f"Document inserted with ID: {result.inserted_id}")
        except Exception as e:
            # Manejo de errores en caso de que la inserción falle
            print(f"Error inserting document: {e}")
            print(f"Full error details: {str(e)}")

# Uso del MongoDBLoader
if __name__ == "__main__":
    loader = MongoDBLoader()
    
    # Probar la conexión primero
    loader.test_connection()
    
    # Ejemplo de datos a cargar
    sample_data = {
        'name': 'Ms. Hannah',
        'last_name': 'Smith',
        'passport': '004078463',
        'email': 'giulia62@tele2.it',
    }

    # Cargar datos en MongoDB
    loader.load(sample_data)

    # Imprimir la URI de MongoDB para verificar
    print(f"MongoDB URI: {MONGODB_CONNECTION_STRING}")