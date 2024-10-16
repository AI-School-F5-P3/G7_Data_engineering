import pg8000
import logging

class SQLLoader:
    def __init__(self, conn_info):
        self.conn = pg8000.connect(**conn_info)
        self.cursor = self.conn.cursor()
        self.logger = logging.getLogger(__name__)

    def load(self, data):
        try:
            self.cursor.execute("""
                INSERT INTO contacts (passport, name, last_name, email, IBAN, salary)
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
                data.get('iban', None),  # IBAN podría no estar presente en tus datos actuales
                data.get('salary', None)  # salary también podría faltar
            ))
            self.conn.commit()
            self.logger.info(f"Data inserted/updated for passport: {data['passport']}")
        except Exception as e:
            self.logger.error(f"Error inserting data into PostgreSQL: {e}")
            self.conn.rollback()
