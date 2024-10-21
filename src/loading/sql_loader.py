import pg8000
import logging

class SQLLoader:
    def __init__(self, conn_info):
        self.conn = pg8000.connect(**conn_info)
        self.cursor = self.conn.cursor()
        self.logger = logging.getLogger(__name__)

    def load_personal_data(self, data):
        try:
            self.cursor.execute("""
                INSERT INTO personal_data (passport, name, last_name, sex, telfnumber, email)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (passport) DO UPDATE
                SET name = EXCLUDED.name,
                    last_name = EXCLUDED.last_name,
                    sex = EXCLUDED.sex,
                    telfnumber = EXCLUDED.telfnumber,
                    email = EXCLUDED.email;
            """, (
                data['passport'],
                data.get('name'),
                data.get('last_name'),
                data.get('sex'),
                data.get('telfnumber'),
                data.get('email')
            ))
            self.conn.commit()
            self.logger.info(f"Personal data inserted/updated for passport: {data['passport']}")
        except Exception as e:
            self.logger.error(f"Error inserting personal data: {e}")
            self.conn.rollback()

    def load_location_data(self, data):
        try:
            self.cursor.execute("""
                INSERT INTO location_data (passport, fullname, city, address)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (passport) DO UPDATE
                SET fullname = EXCLUDED.fullname,
                    city = EXCLUDED.city,
                    address = EXCLUDED.address;
            """, (
                data['passport'],
                data.get('fullname'),
                data.get('city'),
                data.get('address')
            ))
            self.conn.commit()
            self.logger.info(f"Location data inserted/updated for passport: {data['passport']}")
        except Exception as e:
            self.logger.error(f"Error inserting location data: {e}")
            self.conn.rollback()

    def load_professional_data(self, data):
        try:
            self.cursor.execute("""
                INSERT INTO professional_data (passport, fullname, company, company_address, company_telfnumber, company_email, job)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (passport) DO UPDATE
                SET fullname = EXCLUDED.fullname,
                    company = EXCLUDED.company,
                    company_address = EXCLUDED.company_address,
                    company_telfnumber = EXCLUDED.company_telfnumber,
                    company_email = EXCLUDED.company_email,
                    job = EXCLUDED.job;
            """, (
                data['passport'],
                data.get('fullname'),
                data.get('company'),
                data.get('company_address'),
                data.get('company_telfnumber'),
                data.get('company_email'),
                data.get('job')
            ))
            self.conn.commit()
            self.logger.info(f"Professional data inserted/updated for passport: {data['passport']}")
        except Exception as e:
            self.logger.error(f"Error inserting professional data: {e}")
            self.conn.rollback()

    def load_bank_data(self, data):
        try:
            self.cursor.execute("""
                INSERT INTO bank_data (passport, iban, salary)
                VALUES (%s, %s, %s)
                ON CONFLICT (passport) DO UPDATE
                SET iban = EXCLUDED.iban,
                    salary = EXCLUDED.salary;
            """, (
                data['passport'],
                data.get('iban'),
                data.get('salary')
            ))
            self.conn.commit()
            self.logger.info(f"Bank data inserted/updated for passport: {data['passport']}")
        except Exception as e:
            self.logger.error(f"Error inserting bank data: {e}")
            self.conn.rollback()

    def load_net_data(self, data):
        try:
            self.cursor.execute("""
                INSERT INTO net_data (passport, address, ipv4)
                VALUES (%s, %s, %s)
                ON CONFLICT (passport) DO UPDATE
                SET address = EXCLUDED.address,
                    ipv4 = EXCLUDED.ipv4;
            """, (
                data['passport'],
                data.get('address'),
                data.get('ipv4')
            ))
            self.conn.commit()
            self.logger.info(f"Net data inserted/updated for passport: {data['passport']}")
        except Exception as e:
            self.logger.error(f"Error inserting net data: {e}")
            self.conn.rollback()
