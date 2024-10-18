from pymongo import MongoClient

def test_mongo_connection():
    try:
        # Conectar al servidor MongoDB (ajusta la URI según tu configuración)
        client = MongoClient("mongodb://localhost:27017/")  # Cambia el host si usas Docker
        # Listar bases de datos
        databases = client.list_database_names()
        print("Conexión exitosa. Bases de datos disponibles:", databases)
        return True
    except Exception as e:
        print("Error al conectar a MongoDB:", e)
        return False

if __name__ == "__main__":
    test_mongo_connection()
