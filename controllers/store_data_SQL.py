from config.mysql_database import mysql_conn

def store_data_in_mysql(data_list):
    """
    Stores batched user data in MySQL.

    This function takes a batch of processed user data 
    and inserts it into a MySQL table using executemany.

    It first checks that the MySQL connection is alive,
    then constructs the INSERT query with placeholders.

    The data values are extracted from the input dicts
    and inserted using the prepared query.

    The connection is committed and closed at the end.

    Args:
    data_list (list): The batch of processed user data dicts

    Returns:
    None

    Example:

    ```python
    user_data = [
        {'name': 'John', 'age': 30},
        {'name': 'Jane', 'age': 25}  
    ]

    store_data_in_mysql(user_data)
    ```
    """
    if not mysql_conn.is_connected():
        mysql_conn.ping(reconnect=True)
        
    cursor = mysql_conn.cursor()
    insert_query = """
        INSERT INTO users_data (
            IBAN, IPv4, address, city, company, company_address, company_email, company_telfnumber, email, fullname, job, passport, salary, sex, telfnumber
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
    values = [
        (
            data.get('IBAN'),
            data.get('IPv4'),
            data.get('address'),
            data.get('city'),
            data.get('company'),
            data.get('company address'),
            data.get('company_email'),
            data.get('company_telfnumber'),
            data.get('email'),
            data.get('fullname'),
            data.get('job'),
            data.get('passport'),
            data.get('salary'),
            data.get('sex'),
            data.get('telfnumber')
        ) for data in data_list
    ]

    cursor.executemany(insert_query, values)
    mysql_conn.commit()
    cursor.close()