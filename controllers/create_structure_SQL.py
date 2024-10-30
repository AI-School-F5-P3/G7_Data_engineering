from config.mysql_database import mysql_conn


def create_mysql_table():
    """
    Creates a MySQL table to store user data.

    This function connects to a MySQL database, creates a table called 
    'users_data' if it does not already exist, and adds columns for 
    storing user information.

    The table schema includes columns for IBAN, IP address, addresses, 
    company info, contact details, passport number, salary, sex, etc.

    The function handles any errors in creating the table and prints
    the error.

    Args:
    None

    Returns:
    None

    Raises:
    Exception: Any errors creating the MySQL table are raised.

    Example:
    ```python
    import mysql.connector

    cnx = mysql.connector.connect(user='scott', database='mydb')
    create_mysql_table()
    ```
"""

    try:
        mysql_cursor = mysql_conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS users_data (
            IBAN VARCHAR(30),
            IPv4 VARCHAR(20),
            address VARCHAR(255),
            city VARCHAR(255),
            company VARCHAR(255),
            company_address VARCHAR(255),
            company_email VARCHAR(255),
            company_telfnumber VARCHAR(30),
            email VARCHAR(255),
            fullname VARCHAR(255),
            job VARCHAR(255),
            passport VARCHAR(30),
            salary VARCHAR (50),
            sex VARCHAR(30),
            telfnumber VARCHAR(255)

        )
        """

        mysql_cursor.execute(create_table_query)

        mysql_conn.commit()

        mysql_cursor.close()
        mysql_conn.close()
    except Exception as e:
        print(f"Error creating MySQL table: {e}")

