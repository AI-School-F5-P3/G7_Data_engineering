import psycopg2
from psycopg2 import OperationalError

def test_postgres_connection():
    try:
        # Conectar a la base de datos de PostgreSQL (ajusta los parámetros según tu configuración)
        connection = psycopg2.connect(
            user="postgres",        # Cambia 'your_username' por tu nombre de usuario
            password="postgres",    # Cambia 'your_password' por tu contraseña
            host="localhost",            # Cambia si PostgreSQL está en otro host o en Docker
            port="5432",                 # Puerto donde PostgreSQL está escuchando (por defecto 5432)
            database="hr_data"     # Cambia 'your_database' por el nombre de tu base de datos
        )

        # Crear un cursor para ejecutar consultas
        cursor = connection.cursor()

        # Ejecutar una consulta de prueba
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("Conexión exitosa. Versión de PostgreSQL:", record)

        # Cerrar la conexión y cursor
        cursor.close()
        connection.close()

        return True
    except OperationalError as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        return False

if __name__ == "__main__":
    test_postgres_connection()
