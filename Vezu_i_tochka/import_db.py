

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


def show_tables(bd_connection):
    try:
        with bd_connection.cursor() as cursor:
            bd_tables = []
            cursor.execute("""SELECT table_name FROM information_schema.tables
                   WHERE table_schema = 'main'""")
            for table in cursor.fetchall():
                bd_tables.append(table)
            return bd_tables
    except Exception as e:
        print(f"The error '{e}' occurred")


if __name__ == '__main__':
    connection = create_connection(
        'taxi', 'etl_tech_user', 'etl_tech_user_password', 'de-edu-db.chronosavant.ru', '5432'
    )
    tables = show_tables(connection)
    print(tables)
    connection.close()
