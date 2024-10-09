import psycopg2
from config import POSTGRES_URI

class SQLLoader:
    def __init__(self):
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