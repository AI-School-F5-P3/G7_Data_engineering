import psycopg2
import os
from dotenv import load_dotenv
from config import POSTGRES_URI

load_dotenv()  # Esto carga las variables de entorno desde el archivo .env

class SQLLoader:
    def __init__(self):
        # Conectar a la base de datos usando la URI de postgres
        self.conn = psycopg2.connect(POSTGRES_URI)
        self.cursor = self.conn.cursor()

    def load(self, data):
        self.cursor.execute("""
            INSERT INTO hr_data (passport, name, lastname, city, job, salary)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            data['personal_data'].get('passport'),
            data['personal_data'].get('name'),
            data['personal_data'].get('lastname'),
            data['location'].get('city'),
            data['professional_data'].get('job'),
            data['bank_data'].get('salary')
        ))
        self.conn.commit()

    def fetch_all_employees(self):
        # Ejecutar una consulta
        self.cursor.execute("SELECT * FROM employees;")

        # Obtener los resultados
        rows = self.cursor.fetchall()
        return rows

    def close(self):
        # Cerrar el cursor y la conexión
        self.cursor.close()
        self.conn.close()

# Ejemplo de uso
if __name__ == "__main__":
    sql_loader = SQLLoader()

    # Aquí iría tu lógica para cargar datos
    sample_data = {
        'personal_data': {
            'passport': 'A12345678',
            'name': 'John',
            'lastname': 'Doe'
        },
        'location': {
            'city': 'New York'
        },
        'professional_data': {
            'job': 'Engineer'
        },
        'bank_data': {
            'salary': 70000
        }
    }

    sql_loader.load(sample_data)

    # Obtener e imprimir todos los empleados
    employees = sql_loader.fetch_all_employees()
    for emp in employees:
        print(emp)

    # Cerrar la conexión al final
    sql_loader.close()