from create_connection import create_connection


def show_tables(bd_connection):
    try:
        with bd_connection.cursor() as cursor:
            cursor.execute("""SELECT table_name FROM information_schema.tables
                   WHERE table_schema = 'main'""")
            return list(cursor.fetchall())
    except Exception as e:
        print(f"The error '{e}' occurred")


def get_from_table(bd_connection, table):
    try:
        with bd_connection.cursor() as cursor:
            cursor.execute("""SET search_path TO taxi, main;""")
            cursor.execute(f"""SELECT * FROM {table}""")
            return list(cursor.fetchall())

    except Exception as e:
        print(f"The error '{e}' occurred")


if __name__ == '__main__':
    connection = create_connection(
        'taxi', 'etl_tech_user', 'etl_tech_user_password', 'de-edu-db.chronosavant.ru', '5432'
    )
    tables = show_tables(connection)
    print(tables)
    print(get_from_table(connection, 'car_pool'))
    connection.close()
