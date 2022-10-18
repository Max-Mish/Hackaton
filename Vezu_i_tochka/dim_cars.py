from create_connection import create_connection


def upload_dim_cars(connection_origin, connection_ufa):
    def get_from_table(bd_connection):
        try:
            with bd_connection.cursor() as cursor:
                cursor.execute("""SET search_path TO taxi, main;""")
                cursor.execute(
                    """SELECT 
                    plate_num, 
                    model, 
                    revision_dt,
                    update_dt
                    FROM car_pool""")
                return list(cursor.fetchall())

        except Exception as e:
            print(f"The error '{e}' occurred")

    def upload_to_table(bd_connection, bd_data):
        bd_connection.autocommit = False
        for row in bd_data:
            try:
                with bd_connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT NOT EXISTS (SELECT * FROM dim_cars WHERE 
                        plate_num = (%s) and
                        model_name = (%s))
                        """,
                        (row[0], row[1]))
                    if cursor.fetchone()[0]:
                        cursor.execute(
                            """
                            INSERT INTO dim_cars (
                            plate_num, 
                            start_dt, 
                            model_name,
                            revision_dt,
                            deleted_flag
                            ) 
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (row[0], row[3], row[1], row[2], False))
                        bd_connection.commit()
                        print('Row uploaded')

            except Exception as e:
                print(f"The error '{e}' occurred")

    data = get_from_table(connection_origin)
    upload_to_table(connection_ufa, data)


if __name__ == '__main__':
    connection_ufa = create_connection(
        'dwh', 'dwh_ufa', 'dwh_ufa_6x167KSn', 'de-edu-db.chronosavant.ru', '5432'
    )

    connection_origin = create_connection(
        'taxi', 'etl_tech_user', 'etl_tech_user_password', 'de-edu-db.chronosavant.ru', '5432'
    )

    upload_dim_cars(connection_origin, connection_ufa)

    connection_origin.close()
    connection_ufa.close()
