def upload_dim_drivers(connection_download, connection_upload):
    # Функция, осуществляющая импорт данных в таблицу dim_drivers
    try:
        with connection_download.cursor() as cursor:
            cursor.execute("""SET search_path TO taxi, main;""")  # Указываем путь к таблице
            cursor.execute(  # Получаем данные из таблицы drivers исходной БД
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
            data = list(cursor.fetchall())  # Записываем их в data
    except Exception as e:
        print(f"The error '{e}' occurred")

    connection_upload.autocommit = False

    try:
        with connection_upload.cursor() as cursor:
            # Сбрасываем автоинкремент, чтобы при обновлении таблицы personnel_num выдавался корректно
            cursor.execute(
                """ALTER SEQUENCE dim_drivers_personnel_num_seq RESTART WITH 1;
                    UPDATE dim_drivers SET personnel_num=nextval('dim_drivers_personnel_num_seq');"""
            )
        connection_upload.commit()
    except Exception as e:
        print(f"The error '{e}' occurred")

    for row in data:  # Обработка каждой строки данных
        try:
            with connection_upload.cursor() as cursor:
                cursor.execute(  # Проверка на существование строки в таблице dim_drivers
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
                if cursor.fetchone()[0]:  # Если строки в таблице нет
                    cursor.execute(  # Добавляем строку в таблицу dim_cars
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
