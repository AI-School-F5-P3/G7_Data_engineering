import pg8000
import logging
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def get_env_variable(var_name):
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"La variable de entorno {var_name} no está definida")
    return value

class SQLLoader:
    def __init__(self):
        conn_info = {
            'user': get_env_variable('POSTGRES_USER'),
            'password': get_env_variable('POSTGRES_PASSWORD'),
            'host': get_env_variable('POSTGRES_HOST'),
            'port': int(get_env_variable('POSTGRES_PORT')),
            'database': get_env_variable('POSTGRES_DB')
        }
        self.conn = pg8000.connect(**conn_info)
        self.cursor = self.conn.cursor()
        self.logger = logging.getLogger(__name__)

    def load(self, data):
        try:
            self.logger.info(f"Attempting to insert/update data: {data}")
            self.cursor.execute("""
                INSERT INTO hr_data (passport, name, last_name, email, IBAN, salary)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (passport) DO UPDATE
                SET name = EXCLUDED.name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    IBAN = EXCLUDED.IBAN,
                    salary = EXCLUDED.salary;
            """, (
                data['passport'],
                data['name'],
                data['last_name'],
                data['email'],
                data['IBAN'],
                data['salary']
            ))
            self.conn.commit()
            self.logger.info(f"Data inserted/updated for passport: {data['passport']}")
        except Exception as e:
            self.logger.error(f"Error inserting data into PostgreSQL: {e}")
            self.conn.rollback()

def test_connection():
    try:
        conn_info = {
            'user': get_env_variable('POSTGRES_USER'),
            'password': get_env_variable('POSTGRES_PASSWORD'),
            'host': get_env_variable('POSTGRES_HOST'),
            'port': int(get_env_variable('POSTGRES_PORT')),
            'database': get_env_variable('POSTGRES_DB')
        }
        conn = pg8000.connect(**conn_info)
        conn.close()
        print("Conexión exitosa a PostgreSQL")
    except Exception as e:
        print(f"Error al conectar a PostgreSQL: {e}")

if __name__ == "__main__":
    test_connection()
