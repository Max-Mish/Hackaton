from create_connection import create_connection


def upload_dim_clients(connection_download, connection_upload):
    try:
        with connection_download.cursor() as cursor:
            cursor.execute("""SET search_path TO taxi, main;""")
            cursor.execute(
                """SELECT
                    client_phone, 
                    dt,
                    card_num 
                    FROM rides""")
            data = list(cursor.fetchall())
    except Exception as e:
        print(f"The error '{e}' occurred")

    connection_upload.autocommit = False
    for row in data:
        try:
            with connection_upload.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT NOT EXISTS (SELECT * FROM dim_clients WHERE 
                        phone_num = (%s) AND
                        start_dt =(%s) AND
                        card_num=(%s)
                        )
                    """,
                    (row[0], row[1], row[2]))
                if cursor.fetchone()[0]:
                    cursor.execute(
                        """
                        INSERT INTO dim_clients (
                            phone_num,
                            start_dt,
                            card_num,
                            deleted_flag
                            ) 
                        VALUES (%s, %s, %s, %s)
                        """,
                        (row[0], row[1], row[2], False))
                    connection_upload.commit()
                    print('Row uploaded')
        except Exception as e:
            print(f"The error '{e}' occurred")


if __name__ == '__main__':
    connection_ufa = create_connection(
        'dwh', 'dwh_ufa', 'dwh_ufa_6x167KSn', 'de-edu-db.chronosavant.ru', '5432'
    )

    connection_origin = create_connection(
        'taxi', 'etl_tech_user', 'etl_tech_user_password', 'de-edu-db.chronosavant.ru', '5432'
    )

    upload_dim_clients(connection_origin, connection_ufa)

    connection_origin.close()
    connection_ufa.close()
