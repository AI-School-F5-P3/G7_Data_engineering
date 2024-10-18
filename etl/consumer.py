from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_kafka_bootstrap_servers():
    """
    Determine the Kafka bootstrap server address based on environment.
    If the consumer is running inside Docker, use 'kafka:9092', otherwise use 'localhost:29092'.
    """
    if os.environ.get('DOCKER_ENV') == 'true':  # Set this environment variable in Docker Compose
        return 'kafka:9092'
    return 'localhost:29092'

def get_postgres_host():
    """
    Determine the PostgreSQL host address based on environment.
    If the consumer is running inside Docker, use 'postgres', otherwise use 'localhost'.
    """
    if os.environ.get('DOCKER_ENV') == 'true':  # Set this environment variable in Docker Compose
        return 'postgres'
    return 'localhost'

def connect_postgres():
    """
    Establish a connection to PostgreSQL and return the connection object.
    """
    try:
        conn = psycopg2.connect(
            dbname="hr_data",
            user="postgres",
            password="postgres",
            host=get_postgres_host()  # Dynamically get the PostgreSQL host
        )
        logging.info("Connected to PostgreSQL")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        raise e

def create_tables(conn):
    """
    Function to create tables in PostgreSQL if they do not exist.
    """
    create_employee_table = """
    CREATE TABLE IF NOT EXISTS employees (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        last_name VARCHAR(100),
        passport VARCHAR(20)
    );
    """
    
    create_bank_data_table = """
    CREATE TABLE IF NOT EXISTS bank_data (
        id SERIAL PRIMARY KEY,
        passport VARCHAR(20),
        IBAN VARCHAR(34),
        salary VARCHAR(20)
    );
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_employee_table)
            cursor.execute(create_bank_data_table)
            conn.commit()
        logging.info("Tables created (if not existed) in PostgreSQL")
    except psycopg2.Error as e:
        logging.error(f"Error creating tables in PostgreSQL: {e}")
        conn.rollback()

def connect_mongo():
    """
    Establish a connection to MongoDB and return the database object.
    """
    try:
        client = MongoClient("mongodb://mongodb:27017/")  # Internal Docker connection
        logging.info("Connected to MongoDB")
        return client['hr_data']
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        raise e

def consume_and_store(consumer, mongo_db, postgres_conn):
    """
    Function to consume data from Kafka and store it in both MongoDB and PostgreSQL.
    """
    mongo_collections = {
        "personal_data": mongo_db["personal_data"],
        "location": mongo_db["location"],
        "professional_data": mongo_db["professional_data"],
        "bank_data": mongo_db["bank_data"],
        "net_data": mongo_db["net_data"]
    }

    try:
        for message in consumer:
            data = json.loads(message.value)
            logging.info(f"Consumed message: {data}")

            # Insert data into MongoDB based on its type
            if "name" in data:
                mongo_collections["personal_data"].insert_one(data)
            elif "city" in data:
                mongo_collections["location"].insert_one(data)
            elif "company" in data:
                mongo_collections["professional_data"].insert_one(data)
            elif "IBAN" in data:
                mongo_collections["bank_data"].insert_one(data)
            elif "IPv4" in data:
                mongo_collections["net_data"].insert_one(data)

            # Insert relevant data into PostgreSQL
            if "name" in data and "last_name" in data and "passport" in data:
                try:
                    with postgres_conn.cursor() as cursor:
                        cursor.execute(
                            "INSERT INTO employees (name, last_name, passport) VALUES (%s, %s, %s)",
                            (data['name'], data['last_name'], data['passport'])
                        )
                    postgres_conn.commit()
                    logging.info("Data inserted into PostgreSQL")
                except psycopg2.Error as e:
                    logging.error(f"Error inserting data into PostgreSQL: {e}")
                    postgres_conn.rollback()

    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON message: {e}")
    except psycopg2.Error as e:
        logging.error(f"Error inserting data into PostgreSQL: {e}")
        postgres_conn.rollback()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def main():
    # Set up Kafka consumer
    try:
        bootstrap_servers = get_kafka_bootstrap_servers()  # Determine the correct bootstrap servers
        consumer = KafkaConsumer(
            'probando',
            bootstrap_servers=[bootstrap_servers],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='consumer-group-1',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info(f"Kafka consumer connected to {bootstrap_servers}")
    except Exception as e:
        logging.error(f"Error connecting Kafka consumer: {e}")
        raise e

    # Set up MongoDB and PostgreSQL connections
    mongo_db = connect_mongo()

    try:
        with connect_postgres() as postgres_conn:
            create_tables(postgres_conn)  # Ensure tables exist in PostgreSQL

            # Consume messages and store in MongoDB and PostgreSQL
            consume_and_store(consumer, mongo_db, postgres_conn)
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise e

if __name__ == "__main__":
    main()

