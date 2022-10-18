def upload_dim_drivers(connection_download, connection_upload):
    try:
        with connection_download.cursor() as cursor:
            cursor.execute("""SET search_path TO taxi, main;""")
            cursor.execute(
                """SELECT
                    first_name,
                    last_name,
                    middle_name,
                    birth_dt,
                    card_num,
                    driver_license,
                    driver_valid_to,
                    update_dt
                    FROM main.drivers""")
            data = list(cursor.fetchall())
    except Exception as e:
        print(f"The error '{e}' occurred")

    connection_upload.autocommit = False
    for row in data:
        try:
            with connection_upload.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT NOT EXISTS (SELECT * FROM dim_drivers WHERE
                        last_name = (%s) AND
                        first_name = (%s) AND
                        middle_name = (%s) AND
                        birth_dt = (%s) AND
                        card_num = (%s) AND
                        driver_license_num = (%s) AND
                        driver_license_dt = (%s)
                        )
                    """,
                    (row[1], row[0], row[2], row[3], row[4], row[5], row[6]))
                if cursor.fetchone()[0]:
                    cursor.execute(
                        """
                        INSERT INTO dim_drivers (
                            start_dt,
                            last_name,
                            first_name,
                            middle_name,
                            birth_dt,
                            card_num,
                            driver_license_num,
                            driver_license_dt,
                            deleted_flag
                            ) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (row[7], row[1], row[0], row[2], row[3], row[4], row[5], row[6], False))
                    connection_upload.commit()
                    print('Row uploaded')
        except Exception as e:
            print(f"The error '{e}' occurred")
