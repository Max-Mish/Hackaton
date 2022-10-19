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
                        """SELECT * FROM dim_clients WHERE
                            phone_num = (%s) AND
                            deleted_flag = (%s)
                        """,
                        (row[0], False))

                    if cursor.fetchall():
                        cursor.execute(
                            """UPDATE dim_clients SET
                                deleted_flag = (%s),
                                end_dt = (%s)
                                WHERE
                                phone_num = (%s) AND
                                deleted_flag = (%s)
                            """,
                            (True, row[1], row[0], False))
                        connection_upload.commit()

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
