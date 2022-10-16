import psycopg2


def create_connection(db_name, db_user, db_password, db_host, db_port):
    bd_connection_result = None
    try:
        bd_connection_result = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print('Connection to PostgreSQL DB successful')
    except psycopg2.OperationalError as e:
        print(f"The error '{e}' occurred")
    return bd_connection_result
